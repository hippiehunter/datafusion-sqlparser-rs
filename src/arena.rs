// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Document-owned storage for parsed SQL syntax.
//!
//! [`ParsedSql`] is the ownership boundary for arena-backed ASTs. Recursive
//! [`AstBox`] nodes created while parsing a document are allocated from one
//! bump arena. The document exposes only shared statement references, so an
//! arena pointer cannot escape as an owned subtree. Legacy parser entry points
//! remain heap-backed and continue returning owned statements.

use core::borrow::{Borrow, BorrowMut};
use core::cmp::Ordering;
use core::fmt;
use core::hash::{Hash, Hasher};
use core::marker::PhantomData;
use core::ops::{Deref, DerefMut};
use core::ptr::NonNull;

#[cfg(not(feature = "std"))]
use alloc::boxed::Box;

#[cfg(feature = "serde")]
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A recursive AST allocation.
///
/// Heap-backed values behave like `Box<T>`. Arena-backed values are valid only
/// while their owning [`ParsedSql`] is alive; `ParsedSql` never exposes an
/// owned arena subtree. Cloning an arena-backed value outside a parse scope
/// produces a normal heap-backed deep clone.
pub struct AstBox<T> {
    // The low pointer bit records the storage mode. `AstBoxStorage` has at
    // least pointer alignment even when T itself is byte-aligned, so that bit
    // is always available. Keeping this wrapper pointer-sized is important:
    // recursive boxes are embedded throughout the AST, and a separate bool
    // would inflate every parent node (and, recursively, every allocation).
    ptr_and_tag: NonNull<AstBoxStorage<T>>,
    marker: PhantomData<AstBoxStorage<T>>,
}

#[repr(C)]
struct AstBoxStorage<T> {
    // A zero-sized field raises the allocation alignment without consuming
    // payload bytes. The arena tag therefore works for every T, including
    // byte-aligned and zero-sized public uses of AstBox.
    _pointer_alignment: [usize; 0],
    value: T,
}

const ARENA_TAG: usize = 1;

#[cfg(feature = "std")]
thread_local! {
    static AST_DROP_DEPTH: core::cell::Cell<usize> = const { core::cell::Cell::new(0) };
}

impl<T> AstBox<T> {
    /// Allocate a value using the active document arena, or the global heap
    /// when no document parse is active.
    pub fn new(value: T) -> Self {
        let storage = AstBoxStorage {
            _pointer_alignment: [],
            value,
        };

        #[cfg(feature = "std")]
        let storage = match active_arena_alloc(storage) {
            Ok(ptr) => {
                return Self {
                    ptr_and_tag: tag_arena_pointer(ptr),
                    marker: PhantomData,
                };
            }
            Err(storage) => storage,
        };

        let ptr_and_tag = NonNull::from(Box::leak(Box::new(storage)));
        Self {
            ptr_and_tag,
            marker: PhantomData,
        }
    }

    /// Consume the box and return an independently owned value.
    ///
    /// A heap box moves its value without cloning. An arena box is deep-cloned
    /// with arena allocation temporarily disabled so the returned value cannot
    /// retain pointers into its former document.
    pub fn into_owned(this: Self) -> T
    where
        T: Clone,
    {
        if this.is_arena_allocated() {
            #[cfg(feature = "std")]
            let owned = without_active_arena(|| this.deref().clone());
            #[cfg(not(feature = "std"))]
            let owned = this.deref().clone();
            drop(this);
            owned
        } else {
            let ptr = this.storage_ptr().as_ptr();
            core::mem::forget(this);
            // SAFETY: Heap-backed pointers are created with `Box::leak`
            // above, and forgetting `this` prevents Drop from reconstructing
            // the same box.
            unsafe { Box::from_raw(ptr).value }
        }
    }

    /// Move a value out while constructing or consuming an AST inside this
    /// crate. Arena-backed descendants remain tied to the same document, so
    /// this operation is intentionally not public.
    pub(crate) fn into_inner(this: Self) -> T {
        if this.is_arena_allocated() {
            let ptr = this.storage_ptr().as_ptr();
            core::mem::forget(this);
            // SAFETY: The caller moves the uniquely owned value while the
            // active ParsedSql arena remains alive. The abandoned slot is
            // reclaimed with the arena.
            unsafe { core::ptr::addr_of!((*ptr).value).read() }
        } else {
            let ptr = this.storage_ptr().as_ptr();
            core::mem::forget(this);
            // SAFETY: Heap-backed pointers originate from Box::leak.
            unsafe { Box::from_raw(ptr).value }
        }
    }

    /// Whether this node is backed by its parsed document's arena.
    pub fn is_arena_allocated(&self) -> bool {
        self.ptr_and_tag.addr().get() & ARENA_TAG != 0
    }

    fn storage_ptr(&self) -> NonNull<AstBoxStorage<T>> {
        self.ptr_and_tag.map_addr(|address| {
            // SAFETY: clearing a tag from a non-null allocation address
            // cannot produce zero.
            unsafe { core::num::NonZeroUsize::new_unchecked(address.get() & !ARENA_TAG) }
        })
    }
}

#[cfg(feature = "std")]
fn tag_arena_pointer<T>(ptr: NonNull<AstBoxStorage<T>>) -> NonNull<AstBoxStorage<T>> {
    debug_assert_eq!(ptr.addr().get() & ARENA_TAG, 0);
    ptr.map_addr(|address| {
        // SAFETY: setting a bit on a non-zero address remains non-zero.
        unsafe { core::num::NonZeroUsize::new_unchecked(address.get() | ARENA_TAG) }
    })
}

impl<T> Drop for AstBox<T> {
    fn drop(&mut self) {
        #[cfg(feature = "std")]
        {
            AST_DROP_DEPTH.with(|depth| {
                let previous = depth.get();
                depth.set(previous.saturating_add(1));
                struct RestoreDepth<'a> {
                    depth: &'a core::cell::Cell<usize>,
                    previous: usize,
                }
                impl Drop for RestoreDepth<'_> {
                    fn drop(&mut self) {
                        self.depth.set(self.previous);
                    }
                }
                let restore = RestoreDepth { depth, previous };

                // Checking the remaining stack for every AST node is visible
                // in parser benchmarks. Shallow trees need no check; deep
                // recursive destruction samples it periodically so stacker
                // still grows before reaching the guard page.
                if previous >= 16 && previous % 16 == 0 {
                    recursive::__impl::stacker::maybe_grow(
                        recursive::get_minimum_stack_size(),
                        recursive::get_stack_allocation_size(),
                        || self.drop_storage(),
                    );
                } else {
                    self.drop_storage();
                }
                drop(restore);
            });
            return;
        }

        #[cfg(not(feature = "std"))]
        self.drop_storage();
    }
}

impl<T> AstBox<T> {
    fn drop_storage(&mut self) {
        if self.is_arena_allocated() {
            // SAFETY: An arena node is uniquely owned by its parent AST node.
            // Its storage remains live until ParsedSql drops the arena after
            // dropping all statement roots. We run T's destructor but leave
            // deallocation to the arena.
            unsafe {
                core::ptr::drop_in_place(core::ptr::addr_of_mut!(
                    (*self.storage_ptr().as_ptr()).value
                ))
            };
        } else {
            // SAFETY: Heap nodes originate from `Box::leak`, and this is their
            // unique owning AstBox.
            unsafe { drop(Box::from_raw(self.storage_ptr().as_ptr())) };
        }
    }
}

impl<T> Deref for AstBox<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        // SAFETY: Both storage modes keep the pointee live for `self`.
        unsafe { &self.storage_ptr().as_ref().value }
    }
}

impl<T> DerefMut for AstBox<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: AstBox uniquely owns its pointee. Frozen ParsedSql only
        // exposes shared references, so this is reachable for arena nodes only
        // while the parser owns and builds the tree.
        unsafe { &mut self.storage_ptr().as_mut().value }
    }
}

impl<T: Clone> Clone for AstBox<T> {
    fn clone(&self) -> Self {
        Self::new((**self).clone())
    }
}

impl<T: fmt::Debug> fmt::Debug for AstBox<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T: fmt::Display> fmt::Display for AstBox<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
    }
}

impl<T: PartialEq> PartialEq for AstBox<T> {
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}

impl<T: PartialEq> PartialEq<T> for AstBox<T> {
    fn eq(&self, other: &T) -> bool {
        **self == *other
    }
}

impl<T: Eq> Eq for AstBox<T> {}

impl<T: PartialOrd> PartialOrd for AstBox<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        (**self).partial_cmp(&**other)
    }
}

impl<T: Ord> Ord for AstBox<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        (**self).cmp(&**other)
    }
}

impl<T: Hash> Hash for AstBox<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (**self).hash(state);
    }
}

impl<T> AsRef<T> for AstBox<T> {
    fn as_ref(&self) -> &T {
        self
    }
}

impl<T> AsMut<T> for AstBox<T> {
    fn as_mut(&mut self) -> &mut T {
        self
    }
}

impl<T> Borrow<T> for AstBox<T> {
    fn borrow(&self) -> &T {
        self
    }
}

impl<T> BorrowMut<T> for AstBox<T> {
    fn borrow_mut(&mut self) -> &mut T {
        self
    }
}

impl<T> From<T> for AstBox<T> {
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl<T: Default> Default for AstBox<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

// SAFETY: AstBox provides unique ownership. An arena-backed pointer is only
// exposed through ParsedSql, whose frozen arena is Send + Sync.
unsafe impl<T: Send> Send for AstBox<T> {}
// SAFETY: Shared access requires T: Sync, and frozen arena storage is immutable.
unsafe impl<T: Sync> Sync for AstBox<T> {}

#[cfg(feature = "serde")]
impl<T: Serialize> Serialize for AstBox<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        (**self).serialize(serializer)
    }
}

#[cfg(feature = "serde")]
impl<'de, T: Deserialize<'de>> Deserialize<'de> for AstBox<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        T::deserialize(deserializer).map(Self::new)
    }
}

#[cfg(feature = "std")]
mod document {
    use super::*;
    use crate::ast::Statement;
    use crate::dialect::Dialect;
    use crate::parser::{Parser, ParserError};
    use std::alloc::{alloc, dealloc, handle_alloc_error, Layout};
    use std::cell::{Cell, RefCell};
    use std::sync::Arc;

    // Fixed-size chunks trade a small bounded tail for predictable retained
    // memory. Large individual AST nodes receive a right-sized dedicated
    // chunk. The corpus benchmark measures this constant directly.
    const AST_CHUNK_BYTES: usize = 1024;
    const LARGE_CHUNK_GRANULARITY: usize = 64;

    struct AstArenaChunk {
        ptr: NonNull<u8>,
        layout: Layout,
        cursor: usize,
    }

    impl AstArenaChunk {
        fn for_value<T>() -> Self {
            let value_size = core::mem::size_of::<T>().max(1);
            let value_align = core::mem::align_of::<T>();
            let minimum = value_size
                .checked_add(value_align.saturating_sub(1))
                .expect("AST arena allocation size overflow");
            let size = if minimum <= AST_CHUNK_BYTES {
                AST_CHUNK_BYTES
            } else {
                minimum
                    .checked_add(LARGE_CHUNK_GRANULARITY - 1)
                    .expect("AST arena chunk size overflow")
                    / LARGE_CHUNK_GRANULARITY
                    * LARGE_CHUNK_GRANULARITY
            };
            let layout =
                Layout::from_size_align(size, value_align.max(core::mem::align_of::<usize>()))
                    .expect("valid AST arena chunk layout");
            // SAFETY: `layout` is non-zero and valid. Allocation failure is
            // handled with the standard allocation error path.
            let ptr = unsafe { alloc(layout) };
            let ptr = NonNull::new(ptr).unwrap_or_else(|| handle_alloc_error(layout));
            Self {
                ptr,
                layout,
                cursor: 0,
            }
        }

        fn try_alloc<T>(&mut self, value: T) -> Result<NonNull<T>, T> {
            let align = core::mem::align_of::<T>();
            if self.layout.align() < align {
                return Err(value);
            }
            let aligned_cursor = self
                .cursor
                .checked_add(align - 1)
                .map(|cursor| cursor & !(align - 1));
            let Some(start) = aligned_cursor else {
                return Err(value);
            };
            let occupied = core::mem::size_of::<T>().max(1);
            let Some(end) = start.checked_add(occupied) else {
                return Err(value);
            };
            if end > self.layout.size() {
                return Err(value);
            }

            // SAFETY: `start..end` is within this chunk and aligned for T.
            // The bump cursor makes the slot unique for the chunk lifetime.
            let ptr = unsafe { self.ptr.as_ptr().add(start).cast::<T>() };
            // SAFETY: The destination is valid, aligned, and uninitialized.
            unsafe { ptr.write(value) };
            self.cursor = end;
            // SAFETY: A successful global allocation plus an in-bounds offset
            // cannot produce a null pointer.
            Ok(unsafe { NonNull::new_unchecked(ptr) })
        }
    }

    impl Drop for AstArenaChunk {
        fn drop(&mut self) {
            // SAFETY: `ptr` was allocated with exactly `layout`. AST payloads
            // are dropped through their owning AstBox before chunks are freed.
            unsafe { dealloc(self.ptr.as_ptr(), self.layout) };
        }
    }

    struct BuildingAstArena {
        chunks: RefCell<Vec<AstArenaChunk>>,
        committed_bytes: Cell<usize>,
        requested_bytes: Cell<usize>,
        node_allocations: Cell<usize>,
    }

    impl BuildingAstArena {
        fn new() -> Self {
            Self {
                chunks: RefCell::new(Vec::new()),
                committed_bytes: Cell::new(0),
                requested_bytes: Cell::new(0),
                node_allocations: Cell::new(0),
            }
        }

        fn alloc<T>(&self, value: T) -> NonNull<T> {
            let mut chunks = self.chunks.borrow_mut();
            let mut value = Some(value);
            // Reuse aligned tail space in earlier chunks before committing a
            // new one. Documents normally have only a handful of chunks, and
            // this keeps large enum nodes from stranding room that later small
            // nodes can occupy.
            for chunk in chunks.iter_mut().rev() {
                match chunk.try_alloc(value.take().expect("AST value is present")) {
                    Ok(ptr) => return ptr,
                    Err(returned) => value = Some(returned),
                }
            }

            let mut chunk = AstArenaChunk::for_value::<T>();
            let committed = chunk.layout.size();
            let ptr = chunk
                .try_alloc(value.expect("AST value is present"))
                .unwrap_or_else(|_| unreachable!("fresh AST arena chunk fits its value"));
            chunks.push(chunk);
            self.committed_bytes.set(
                self.committed_bytes
                    .get()
                    .checked_add(committed)
                    .expect("AST arena committed byte count overflow"),
            );
            ptr
        }

        fn freeze(self) -> FrozenAstArena {
            FrozenAstArena {
                _chunks: self.chunks.into_inner(),
                committed_bytes: self.committed_bytes.get(),
                requested_bytes: self.requested_bytes.get(),
                node_allocations: self.node_allocations.get(),
            }
        }
    }

    thread_local! {
        static ACTIVE_ARENA: Cell<*const BuildingAstArena> =
            const { Cell::new(core::ptr::null()) };
    }

    pub(super) fn active_arena_alloc<T>(value: T) -> Result<NonNull<T>, T> {
        ACTIVE_ARENA.with(|active| {
            let arena = active.get();
            if arena.is_null() {
                Err(value)
            } else {
                // SAFETY: ACTIVE_ARENA is installed by `with_arena` for the
                // dynamic extent of parsing. The building arena outlives every
                // allocation returned during that scope.
                let arena = unsafe { &*arena };
                arena.requested_bytes.set(
                    arena
                        .requested_bytes
                        .get()
                        .saturating_add(core::mem::size_of::<T>()),
                );
                arena
                    .node_allocations
                    .set(arena.node_allocations.get().saturating_add(1));
                Ok(arena.alloc(value))
            }
        })
    }

    pub(super) fn without_active_arena<T>(f: impl FnOnce() -> T) -> T {
        ACTIVE_ARENA.with(|active| {
            let previous = active.replace(core::ptr::null());
            struct Restore<'a> {
                active: &'a Cell<*const BuildingAstArena>,
                previous: *const BuildingAstArena,
            }
            impl Drop for Restore<'_> {
                fn drop(&mut self) {
                    self.active.set(self.previous);
                }
            }
            let restore = Restore { active, previous };
            let result = f();
            drop(restore);
            result
        })
    }

    fn with_arena<T>(arena: &BuildingAstArena, f: impl FnOnce() -> T) -> T {
        ACTIVE_ARENA.with(|active| {
            let previous = active.replace(arena as *const BuildingAstArena);
            struct Restore<'a> {
                active: &'a Cell<*const BuildingAstArena>,
                previous: *const BuildingAstArena,
            }
            impl Drop for Restore<'_> {
                fn drop(&mut self) {
                    self.active.set(self.previous);
                }
            }
            let restore = Restore { active, previous };
            let result = f();
            drop(restore);
            result
        })
    }

    /// Allocation statistics for a frozen syntax document.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct AstArenaStats {
        /// Number of recursive AST nodes allocated from the arena.
        pub node_allocations: usize,
        /// Payload bytes requested by recursive AST nodes.
        pub requested_bytes: usize,
        /// Bytes committed in arena chunks for recursive AST nodes.
        pub committed_bytes: usize,
        /// Committed bytes not occupied by requested node payloads.
        pub slack_bytes: usize,
    }

    struct FrozenAstArena {
        _chunks: Vec<AstArenaChunk>,
        committed_bytes: usize,
        requested_bytes: usize,
        node_allocations: usize,
    }

    impl FrozenAstArena {
        fn stats(&self) -> AstArenaStats {
            AstArenaStats {
                node_allocations: self.node_allocations,
                requested_bytes: self.requested_bytes,
                committed_bytes: self.committed_bytes,
                slack_bytes: self.committed_bytes.saturating_sub(self.requested_bytes),
            }
        }
    }

    // SAFETY: FrozenAstArena exposes no allocation or mutable access after
    // construction. AstBox pointees are read-only through ParsedSql.
    unsafe impl Sync for FrozenAstArena {}
    // SAFETY: Frozen chunks are immutable and their allocations may be freed
    // on any thread supported by the global allocator.
    unsafe impl Send for FrozenAstArena {}

    /// An owned SQL source and its frozen parsed statement batch.
    pub struct ParsedSql {
        // Statements must drop before the bump storage they reference. Struct
        // fields are dropped in declaration order.
        statements: Vec<Statement>,
        source: Arc<str>,
        arena: FrozenAstArena,
    }

    impl ParsedSql {
        /// Parse SQL into one shareable, arena-owned syntax document.
        pub fn parse(
            dialect: &dyn Dialect,
            source: impl Into<Arc<str>>,
        ) -> Result<Arc<Self>, ParserError> {
            // SAFETY: The no-op callback cannot move an arena node out of the
            // document under construction.
            unsafe { Self::parse_and_edit(dialect, source, |_| ()) }.map(|(document, ())| document)
        }

        /// Parse and mutate a document while its arena is still in the
        /// building state, then freeze it into a shareable owner.
        ///
        /// Recursive nodes created by `edit` are allocated in the same arena
        /// as parser-created nodes. The callback result can itself be a
        /// consumer-specific `Result`, allowing callers to preserve their own
        /// error type without coupling it to [`ParserError`].
        /// # Safety
        ///
        /// `edit` must not move or copy an arena-backed AST node anywhere that
        /// can outlive the returned document. Any callback result containing
        /// syntax must remain owned by, and be destroyed before, that document.
        pub unsafe fn parse_and_edit<R>(
            dialect: &dyn Dialect,
            source: impl Into<Arc<str>>,
            edit: impl FnOnce(&mut [Statement]) -> R,
        ) -> Result<(Arc<Self>, R), ParserError> {
            let source = source.into();
            let arena = BuildingAstArena::new();
            let (statements, edit_result) = with_arena(&arena, || {
                let mut statements = Parser::parse_sql(dialect, &source)?;
                let edit_result = edit(&mut statements);
                Ok::<_, ParserError>((statements, edit_result))
            })?;
            let document = Arc::new(Self {
                statements,
                source,
                arena: arena.freeze(),
            });
            Ok((document, edit_result))
        }

        /// Clone this document into a new arena, apply one edit session, and
        /// freeze the result. The source buffer is shared between documents.
        /// # Safety
        ///
        /// `edit` must not let an arena-backed node escape independently of
        /// the returned document. See [`Self::parse_and_edit`].
        pub unsafe fn rewrite<R>(
            &self,
            edit: impl FnOnce(&mut [Statement]) -> R,
        ) -> (Arc<Self>, R) {
            let arena = BuildingAstArena::new();
            let (statements, edit_result) = with_arena(&arena, || {
                let mut statements = self.statements.clone();
                let edit_result = edit(&mut statements);
                (statements, edit_result)
            });
            let document = Arc::new(Self {
                statements,
                source: Arc::clone(&self.source),
                arena: arena.freeze(),
            });
            (document, edit_result)
        }

        /// Adopt an already-owned statement batch behind the same document
        /// boundary used by arena parses.
        ///
        /// Recursive nodes in `statements` retain their existing storage
        /// mode. This is primarily an interoperability bridge for callers
        /// that construct syntax programmatically; parsed input should use
        /// [`Self::parse`] or [`Self::parse_and_edit`] so its recursive nodes
        /// are arena-backed.
        pub fn from_statements(
            source: impl Into<Arc<str>>,
            statements: Vec<Statement>,
        ) -> Arc<Self> {
            // SAFETY: The no-op callback cannot let syntax escape.
            unsafe { Self::from_statements_and_edit(source, statements, |_| ()) }.0
        }

        /// Adopt and mutate an already-owned statement batch before freezing
        /// it behind a document owner.
        ///
        /// Existing recursive nodes preserve their current storage. Recursive
        /// nodes created by `edit` are allocated in this document's arena.
        /// # Safety
        ///
        /// `edit` must not let an arena-backed node escape independently of
        /// the returned document. See [`Self::parse_and_edit`].
        pub unsafe fn from_statements_and_edit<R>(
            source: impl Into<Arc<str>>,
            mut statements: Vec<Statement>,
            edit: impl FnOnce(&mut [Statement]) -> R,
        ) -> (Arc<Self>, R) {
            let source = source.into();
            let arena = BuildingAstArena::new();
            let edit_result = with_arena(&arena, || edit(&mut statements));
            let document = Arc::new(Self {
                statements,
                source,
                arena: arena.freeze(),
            });
            (document, edit_result)
        }

        /// Parsed statement roots. Their lifetime is tied to this document.
        pub fn statements(&self) -> &[Statement] {
            &self.statements
        }

        /// Original SQL source retained by the document.
        pub fn source(&self) -> &str {
            &self.source
        }

        /// Recursive-node arena statistics.
        pub fn arena_stats(&self) -> AstArenaStats {
            self.arena.stats()
        }

        /// Create a cheap owned handle to one statement root.
        pub fn statement(self: &Arc<Self>, index: usize) -> Option<StatementHandle> {
            (index < self.statements.len()).then(|| StatementHandle {
                document: Arc::clone(self),
                index,
            })
        }
    }

    impl Deref for ParsedSql {
        type Target = [Statement];

        fn deref(&self) -> &Self::Target {
            self.statements()
        }
    }

    impl AsRef<[Statement]> for ParsedSql {
        fn as_ref(&self) -> &[Statement] {
            self.statements()
        }
    }

    /// A shareable document-and-index handle to one statement root.
    #[derive(Clone)]
    pub struct StatementHandle {
        document: Arc<ParsedSql>,
        index: usize,
    }

    impl StatementHandle {
        /// Borrow the statement for the lifetime of this handle.
        pub fn get(&self) -> &Statement {
            &self.document.statements[self.index]
        }

        /// Zero-based root index within the parsed document.
        pub fn index(&self) -> usize {
            self.index
        }

        /// Owning parsed document.
        pub fn document(&self) -> &Arc<ParsedSql> {
            &self.document
        }
    }

    impl Deref for StatementHandle {
        type Target = Statement;

        fn deref(&self) -> &Self::Target {
            self.get()
        }
    }

    impl AsRef<Statement> for StatementHandle {
        fn as_ref(&self) -> &Statement {
            self.get()
        }
    }

    impl Borrow<Statement> for StatementHandle {
        fn borrow(&self) -> &Statement {
            self.get()
        }
    }

    impl fmt::Display for StatementHandle {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            fmt::Display::fmt(self.get(), f)
        }
    }

    impl fmt::Debug for StatementHandle {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("StatementHandle")
                .field("index", &self.index)
                .field("statement", &self.get())
                .finish()
        }
    }

    impl fmt::Debug for ParsedSql {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("ParsedSql")
                .field("source", &self.source)
                .field("statements", &self.statements)
                .field("arena", &self.arena.stats())
                .finish()
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::ast::Statement;
        use crate::dialect::PostgreSqlDialect;

        #[test]
        fn ast_box_is_pointer_sized_for_all_payload_alignments() {
            assert_eq!(
                core::mem::size_of::<AstBox<Statement>>(),
                core::mem::size_of::<usize>()
            );
            assert_eq!(
                core::mem::size_of::<AstBox<u8>>(),
                core::mem::size_of::<usize>()
            );
        }

        #[test]
        fn document_matches_owned_parser_and_uses_arena_boxes() {
            let dialect = PostgreSqlDialect {};
            let sql = "SELECT a + 1 FROM t WHERE b IN (SELECT b FROM u)";
            let owned = Parser::parse_sql(&dialect, sql).unwrap();
            let document = ParsedSql::parse(&dialect, Arc::<str>::from(sql)).unwrap();

            assert_eq!(document.statements(), owned.as_slice());
            assert_eq!(document.source(), sql);
            assert!(document.arena_stats().committed_bytes > 0);
            assert!(document.arena_stats().requested_bytes > 0);
            assert!(document.arena_stats().node_allocations > 0);
            let Statement::Query(query) = &document.statements()[0] else {
                panic!("expected query")
            };
            assert!(query.is_arena_allocated());
        }

        #[test]
        fn document_is_shareable_across_threads() {
            let document = ParsedSql::parse(
                &PostgreSqlDialect {},
                Arc::<str>::from("SELECT 1 UNION ALL SELECT 2"),
            )
            .unwrap();
            let worker_document = Arc::clone(&document);
            let rendered = std::thread::spawn(move || worker_document.statements()[0].to_string())
                .join()
                .unwrap();
            assert_eq!(rendered, "SELECT 1 UNION ALL SELECT 2");
        }

        #[test]
        fn cloning_an_arena_box_produces_independent_heap_storage() {
            let document =
                ParsedSql::parse(&PostgreSqlDialect {}, Arc::<str>::from("SELECT (1 + 2)"))
                    .unwrap();
            let Statement::Query(query) = &document.statements()[0] else {
                panic!("expected query")
            };
            let cloned = query.clone();
            assert!(!cloned.is_arena_allocated());
            drop(document);
            assert_eq!(cloned.to_string(), "SELECT (1 + 2)");
        }

        #[test]
        fn edit_and_rewrite_keep_recursive_nodes_in_the_document_arena() {
            let dialect = PostgreSqlDialect {};
            // SAFETY: The callback only mutates nodes owned by the document.
            let (document, ()) = unsafe {
                ParsedSql::parse_and_edit(&dialect, Arc::<str>::from("SELECT 1"), |statements| {
                    let Statement::Query(query) = &mut statements[0] else {
                        panic!("expected query")
                    };
                    query.locks.push(crate::ast::LockClause {
                        lock_type: crate::ast::LockType::Update,
                        of: None,
                        nonblock: None,
                    });
                })
            }
            .unwrap();
            assert!(document.statements()[0].to_string().contains("FOR UPDATE"));

            // SAFETY: The callback only mutates nodes owned by the document.
            let (rewritten, ()) = unsafe {
                document.rewrite(|statements| {
                    let Statement::Query(query) = &mut statements[0] else {
                        panic!("expected query")
                    };
                    query.body = AstBox::new(crate::ast::SetExpr::Values(crate::ast::Values {
                        explicit_row: false,
                        rows: vec![vec![crate::ast::Expr::value(crate::test_utils::number(
                            "2",
                        ))]],
                        value_keyword: false,
                    }));
                })
            };
            let Statement::Query(query) = &rewritten.statements()[0] else {
                panic!("expected query")
            };
            assert!(query.body.is_arena_allocated());
            assert_eq!(
                rewritten.statements()[0].to_string(),
                "VALUES (2) FOR UPDATE"
            );
        }

        #[test]
        fn heap_scratch_scope_restores_document_arena_after_panic() {
            let dialect = PostgreSqlDialect {};
            // SAFETY: The callback only inspects allocation modes and does not
            // let any arena-backed syntax escape the returned document.
            let (document, ()) = unsafe {
                ParsedSql::parse_and_edit(&dialect, Arc::<str>::from("SELECT 1"), |_statements| {
                    let before = AstBox::new(1_u8);
                    assert!(before.is_arena_allocated());

                    let scratch = crate::arena::with_heap_ast_allocations(|| AstBox::new(2_u8));
                    assert!(!scratch.is_arena_allocated());

                    let panic_result = std::panic::catch_unwind(|| {
                        crate::arena::with_heap_ast_allocations(|| {
                            let scratch = AstBox::new(3_u8);
                            assert!(!scratch.is_arena_allocated());
                            panic!("exercise scope restoration");
                        });
                    });
                    assert!(panic_result.is_err());

                    let after = AstBox::new(4_u8);
                    assert!(after.is_arena_allocated());
                })
            }
            .unwrap();
            assert_eq!(document.statements()[0].to_string(), "SELECT 1");
        }

        #[test]
        fn statement_handle_keeps_document_alive() {
            let document =
                ParsedSql::parse(&PostgreSqlDialect {}, Arc::<str>::from("SELECT 42")).unwrap();
            let handle = document.statement(0).unwrap();
            drop(document);
            assert_eq!(handle.get().to_string(), "SELECT 42");
            assert_eq!(handle.index(), 0);
        }

        #[cfg(feature = "std")]
        #[test]
        fn deeply_nested_document_drops_without_stack_overflow() {
            let predicate = (0..8192)
                .map(|index| format!("id = {index}"))
                .collect::<Vec<_>>()
                .join(" OR ");
            let sql = format!("SELECT id FROM t WHERE {predicate}");
            let document = ParsedSql::parse(&PostgreSqlDialect {}, Arc::<str>::from(sql)).unwrap();
            assert_eq!(document.statements().len(), 1);
            drop(document);
        }

        #[cfg(feature = "std")]
        #[test]
        fn deeply_nested_heap_expression_drops_without_stack_overflow() {
            let expression = (0..8192)
                .map(|index| format!("id = {index}"))
                .collect::<Vec<_>>()
                .join(" OR ");
            let dialect = PostgreSqlDialect {};
            let parser = Parser::new(&dialect).try_with_sql(&expression).unwrap();
            let expression = parser.parse_expr().unwrap();
            drop(expression);
        }
    }
}

#[cfg(feature = "std")]
use document::{active_arena_alloc, without_active_arena};
#[cfg(feature = "std")]
pub use document::{AstArenaStats, ParsedSql, StatementHandle};

/// Run temporary AST work with document-arena allocation suspended.
///
/// This is intended for scratch transformations invoked from
/// [`ParsedSql::parse_and_edit`] or [`ParsedSql::rewrite`]. Recursive nodes
/// created by `f` use the global heap, so dropping a temporary clone reclaims
/// its storage immediately instead of stranding it in the document arena.
/// The previous allocation scope is restored even if `f` panics.
#[cfg(feature = "std")]
pub fn with_heap_ast_allocations<T>(f: impl FnOnce() -> T) -> T {
    without_active_arena(f)
}
