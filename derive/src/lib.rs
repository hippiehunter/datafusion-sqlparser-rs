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

use proc_macro2::TokenStream;
use quote::{format_ident, quote, quote_spanned, ToTokens};
use syn::spanned::Spanned;
use syn::{
    parse::{Parse, ParseStream},
    parse_macro_input, parse_quote, Attribute, Data, DeriveInput, Fields, GenericParam, Generics,
    Ident, Index, LitStr, Meta, Token, Type, TypePath,
};
use syn::{Path, PathArguments};

/// Implementation of `#[derive(NoInlineClone)]`.
///
/// Rust's built-in `Clone` derive marks the generated implementation as
/// cross-crate inlineable. That is normally a good default, but it causes a
/// large recursive AST enum to bring a complete clone graph into every crate
/// that clones it. This derive is semantically equivalent to `Clone` while
/// deliberately keeping the implementation in the crate that owns the type.
#[proc_macro_derive(NoInlineClone)]
pub fn derive_no_inline_clone(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let generics = add_clone_bounds(input.generics.clone());
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let body = clone_body(&input.data);

    proc_macro::TokenStream::from(quote! {
        impl #impl_generics ::core::clone::Clone for #name #ty_generics #where_clause {
            #[inline(never)]
            fn clone(&self) -> Self {
                #body
            }
        }
    })
}

fn add_clone_bounds(mut generics: Generics) -> Generics {
    for param in &mut generics.params {
        if let GenericParam::Type(ref mut type_param) = *param {
            type_param.bounds.push(parse_quote!(::core::clone::Clone));
        }
    }
    generics
}

fn clone_body(data: &Data) -> TokenStream {
    match data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => {
                let cloned = fields.named.iter().map(|field| {
                    let name = &field.ident;
                    quote_spanned!(field.span() => #name: ::core::clone::Clone::clone(&self.#name))
                });
                quote!(Self { #(#cloned),* })
            }
            Fields::Unnamed(fields) => {
                let cloned = fields.unnamed.iter().enumerate().map(|(index, field)| {
                    let index = Index::from(index);
                    quote_spanned!(field.span() => ::core::clone::Clone::clone(&self.#index))
                });
                quote!(Self(#(#cloned),*))
            }
            Fields::Unit => quote!(Self),
        },
        Data::Enum(data) => {
            let arms = data.variants.iter().map(|variant| {
                let variant_name = &variant.ident;
                match &variant.fields {
                    Fields::Named(fields) => {
                        let names = fields.named.iter().map(|field| &field.ident);
                        let cloned = fields.named.iter().map(|field| {
                            let name = &field.ident;
                            quote_spanned!(field.span() => #name: ::core::clone::Clone::clone(#name))
                        });
                        quote!(Self::#variant_name { #(#names),* } => Self::#variant_name { #(#cloned),* })
                    }
                    Fields::Unnamed(fields) => {
                        let names = fields.unnamed.iter().enumerate().map(|(index, field)| {
                            format_ident!("field_{index}", span = field.span())
                        });
                        let cloned = fields.unnamed.iter().enumerate().map(|(index, field)| {
                            let name = format_ident!("field_{index}", span = field.span());
                            quote_spanned!(field.span() => ::core::clone::Clone::clone(#name))
                        });
                        quote!(Self::#variant_name(#(#names),*) => Self::#variant_name(#(#cloned),*))
                    }
                    Fields::Unit => quote!(Self::#variant_name => Self::#variant_name),
                }
            });
            quote!(match self { #(#arms),* })
        }
        Data::Union(data) => syn::Error::new(
            data.union_token.span(),
            "NoInlineClone cannot be derived for unions",
        )
        .to_compile_error(),
    }
}

/// Implementation of `#[derive(NoInlineDebug)]`.
///
/// The formatting contract matches the built-in `Debug` derive, including
/// alternate/pretty formatting through the standard debug builders, but the
/// implementation remains owned by the crate that defines the AST type.
#[proc_macro_derive(NoInlineDebug)]
pub fn derive_no_inline_debug(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let generics = add_debug_bounds(input.generics.clone());
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let body = debug_body(name, &input.data);

    proc_macro::TokenStream::from(quote! {
        impl #impl_generics ::core::fmt::Debug for #name #ty_generics #where_clause {
            #[inline(never)]
            fn fmt(
                &self,
                formatter: &mut ::core::fmt::Formatter<'_>,
            ) -> ::core::fmt::Result {
                #body
            }
        }
    })
}

fn add_debug_bounds(mut generics: Generics) -> Generics {
    for param in &mut generics.params {
        if let GenericParam::Type(ref mut type_param) = *param {
            type_param.bounds.push(parse_quote!(::core::fmt::Debug));
        }
    }
    generics
}

fn debug_body(type_name: &Ident, data: &Data) -> TokenStream {
    match data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => {
                let names: Vec<_> = fields.named.iter().map(|field| &field.ident).collect();
                quote! {
                    let mut debug = formatter.debug_struct(stringify!(#type_name));
                    #(debug.field(stringify!(#names), &self.#names);)*
                    debug.finish()
                }
            }
            Fields::Unnamed(fields) => {
                let indices: Vec<_> = (0..fields.unnamed.len()).map(Index::from).collect();
                quote! {
                    let mut debug = formatter.debug_tuple(stringify!(#type_name));
                    #(debug.field(&self.#indices);)*
                    debug.finish()
                }
            }
            Fields::Unit => quote!(formatter.write_str(stringify!(#type_name))),
        },
        Data::Enum(data) => {
            let arms = data.variants.iter().map(|variant| {
                let variant_name = &variant.ident;
                match &variant.fields {
                    Fields::Named(fields) => {
                        let names: Vec<_> =
                            fields.named.iter().map(|field| &field.ident).collect();
                        quote! {
                            Self::#variant_name { #(#names),* } => {
                                let mut debug = formatter.debug_struct(stringify!(#variant_name));
                                #(debug.field(stringify!(#names), #names);)*
                                debug.finish()
                            }
                        }
                    }
                    Fields::Unnamed(fields) => {
                        let names: Vec<_> = fields
                            .unnamed
                            .iter()
                            .enumerate()
                            .map(|(index, field)| {
                                format_ident!("field_{index}", span = field.span())
                            })
                            .collect();
                        quote! {
                            Self::#variant_name(#(#names),*) => {
                                let mut debug = formatter.debug_tuple(stringify!(#variant_name));
                                #(debug.field(#names);)*
                                debug.finish()
                            }
                        }
                    }
                    Fields::Unit => {
                        quote!(Self::#variant_name => formatter.write_str(stringify!(#variant_name)))
                    }
                }
            });
            quote!(match self { #(#arms),* })
        }
        Data::Union(data) => syn::Error::new(
            data.union_token.span(),
            "NoInlineDebug cannot be derived for unions",
        )
        .to_compile_error(),
    }
}

/// Implementation of `#[derive(NoInlinePartialEq)]`.
///
/// Fields are compared in declaration order and short-circuit exactly as in
/// Rust's built-in `PartialEq` derive. The generated method is deliberately
/// out of line so recursive AST equality is not instantiated by every user.
#[proc_macro_derive(NoInlinePartialEq)]
pub fn derive_no_inline_partial_eq(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let generics = add_partial_eq_bounds(input.generics.clone());
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let body = partial_eq_body(&input.data);

    proc_macro::TokenStream::from(quote! {
        impl #impl_generics ::core::cmp::PartialEq for #name #ty_generics #where_clause {
            #[inline(never)]
            fn eq(&self, other: &Self) -> bool {
                #body
            }
        }
    })
}

fn add_partial_eq_bounds(mut generics: Generics) -> Generics {
    for param in &mut generics.params {
        if let GenericParam::Type(ref mut type_param) = *param {
            type_param.bounds.push(parse_quote!(::core::cmp::PartialEq));
        }
    }
    generics
}

fn partial_eq_body(data: &Data) -> TokenStream {
    match data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => {
                let names: Vec<_> = fields.named.iter().map(|field| &field.ident).collect();
                quote!(true #(&& ::core::cmp::PartialEq::eq(&self.#names, &other.#names))*)
            }
            Fields::Unnamed(fields) => {
                let indices: Vec<_> = (0..fields.unnamed.len()).map(Index::from).collect();
                quote!(true #(&& ::core::cmp::PartialEq::eq(&self.#indices, &other.#indices))*)
            }
            Fields::Unit => quote!(true),
        },
        Data::Enum(data) => {
            let arms = data.variants.iter().map(|variant| {
                let variant_name = &variant.ident;
                match &variant.fields {
                    Fields::Named(fields) => {
                        let left_names: Vec<_> = fields
                            .named
                            .iter()
                            .enumerate()
                            .map(|(index, field)| {
                                format_ident!("left_{index}", span = field.span())
                            })
                            .collect();
                        let right_names: Vec<_> = fields
                            .named
                            .iter()
                            .enumerate()
                            .map(|(index, field)| {
                                format_ident!("right_{index}", span = field.span())
                            })
                            .collect();
                        let field_names: Vec<_> =
                            fields.named.iter().map(|field| &field.ident).collect();
                        quote! {
                            (
                                Self::#variant_name { #(#field_names: #left_names),* },
                                Self::#variant_name { #(#field_names: #right_names),* },
                            ) => true #(&& ::core::cmp::PartialEq::eq(#left_names, #right_names))*
                        }
                    }
                    Fields::Unnamed(fields) => {
                        let left_names: Vec<_> = fields
                            .unnamed
                            .iter()
                            .enumerate()
                            .map(|(index, field)| {
                                format_ident!("left_{index}", span = field.span())
                            })
                            .collect();
                        let right_names: Vec<_> = fields
                            .unnamed
                            .iter()
                            .enumerate()
                            .map(|(index, field)| {
                                format_ident!("right_{index}", span = field.span())
                            })
                            .collect();
                        quote! {
                            (
                                Self::#variant_name(#(#left_names),*),
                                Self::#variant_name(#(#right_names),*),
                            ) => true #(&& ::core::cmp::PartialEq::eq(#left_names, #right_names))*
                        }
                    }
                    Fields::Unit => {
                        quote!((Self::#variant_name, Self::#variant_name) => true)
                    }
                }
            });
            quote! {
                match (self, other) {
                    #(#arms),*,
                    _ => false,
                }
            }
        }
        Data::Union(data) => syn::Error::new(
            data.union_token.span(),
            "NoInlinePartialEq cannot be derived for unions",
        )
        .to_compile_error(),
    }
}

/// Implementation of `#[derive(VisitMut)]`.
///
/// The generated recursive implementation is deliberately non-generic over
/// the concrete visitor. `sqlparser::ast::VisitMut` supplies the compatible
/// typed-break wrapper; generating the recursion for that generic wrapper
/// would duplicate the entire AST walk for every visitor type downstream.
#[proc_macro_derive(VisitMut, attributes(visit))]
pub fn derive_visit_mut(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    derive_visit(
        input,
        &VisitType {
            visit_trait: quote!(VisitMutErased),
            visitor_trait: quote!(VisitorMut),
            visit_method: quote!(visit_mut_erased),
            modifier: Some(quote!(mut)),
        },
    )
}

/// Implementation of `#[derive(Visit)]`; see [`derive_visit_mut`].
#[proc_macro_derive(Visit, attributes(visit))]
pub fn derive_visit_immutable(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    derive_visit(
        input,
        &VisitType {
            visit_trait: quote!(VisitErased),
            visitor_trait: quote!(Visitor),
            visit_method: quote!(visit_erased),
            modifier: None,
        },
    )
}

struct VisitType {
    visit_trait: TokenStream,
    visitor_trait: TokenStream,
    visit_method: TokenStream,
    modifier: Option<TokenStream>,
}

fn derive_visit(input: proc_macro::TokenStream, visit_type: &VisitType) -> proc_macro::TokenStream {
    // Parse the input tokens into a syntax tree.
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;

    let VisitType {
        visit_trait,
        visitor_trait,
        visit_method,
        modifier,
    } = visit_type;

    let attributes = Attributes::parse(&input.attrs);
    // Add the erased traversal bound to every type parameter.
    let generics = add_trait_bounds(input.generics, visit_type);
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let (pre_visit, post_visit) = attributes.visit(quote!(self));
    let children = visit_children(&input.data, visit_type);

    let expanded = quote! {
        // The generated impl.
        // Stack growth is applied by the `Vec<T>` and `AstBox<T>` visitor
        // implementations. Every recursive owned AST cycle must cross one of
        // those indirections, so wrapping each derived leaf as well only
        // duplicates stacker machinery without adding protection.
        impl #impl_generics sqlparser::ast::#visit_trait for #name #ty_generics #where_clause {
            fn #visit_method(
                &#modifier self,
                visitor: &mut dyn sqlparser::ast::#visitor_trait<Break = ()>
            ) -> ::std::ops::ControlFlow<()> {
                #pre_visit
                #children
                #post_visit
                ::std::ops::ControlFlow::Continue(())
            }
        }
    };

    proc_macro::TokenStream::from(expanded)
}

/// Parses attributes that can be provided to this macro
///
/// `#[visit(leaf, with = "visit_expr")]`
#[derive(Default)]
struct Attributes {
    /// Content for the `with` attribute
    with: Option<Ident>,
}

struct WithIdent {
    with: Option<Ident>,
}
impl Parse for WithIdent {
    fn parse(input: ParseStream) -> Result<Self, syn::Error> {
        let mut result = WithIdent { with: None };
        let ident = input.parse::<Ident>()?;
        if ident != "with" {
            return Err(syn::Error::new(
                ident.span(),
                "Expected identifier to be `with`",
            ));
        }
        input.parse::<Token!(=)>()?;
        let s = input.parse::<LitStr>()?;
        result.with = Some(format_ident!("{}", s.value(), span = s.span()));
        Ok(result)
    }
}

impl Attributes {
    fn parse(attrs: &[Attribute]) -> Self {
        let mut out = Self::default();
        for attr in attrs {
            if let Meta::List(ref metalist) = attr.meta {
                if metalist.path.is_ident("visit") {
                    match syn::parse2::<WithIdent>(metalist.tokens.clone()) {
                        Ok(with_ident) => {
                            out.with = with_ident.with;
                        }
                        Err(e) => {
                            panic!("{}", e);
                        }
                    }
                }
            }
        }
        out
    }

    /// Returns the pre and post visit token streams
    fn visit(&self, s: TokenStream) -> (Option<TokenStream>, Option<TokenStream>) {
        let pre_visit = self.with.as_ref().map(|m| {
            let m = format_ident!("pre_{}", m);
            quote!(visitor.#m(#s)?;)
        });
        let post_visit = self.with.as_ref().map(|m| {
            let m = format_ident!("post_{}", m);
            quote!(visitor.#m(#s)?;)
        });
        (pre_visit, post_visit)
    }
}

// Add the erased traversal bound to every type parameter.
fn add_trait_bounds(mut generics: Generics, VisitType { visit_trait, .. }: &VisitType) -> Generics {
    for param in &mut generics.params {
        if let GenericParam::Type(ref mut type_param) = *param {
            type_param
                .bounds
                .push(parse_quote!(sqlparser::ast::#visit_trait));
        }
    }
    generics
}

// Generate the body of the visit implementation for the given type
fn visit_children(
    data: &Data,
    VisitType {
        visit_trait,
        visit_method,
        modifier,
        ..
    }: &VisitType,
) -> TokenStream {
    match data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => {
                let recurse = fields.named.iter().map(|f| {
                    let name = &f.ident;
                    let is_option = is_option(&f.ty);
                    let attributes = Attributes::parse(&f.attrs);
                    if is_option && attributes.with.is_some() {
                        let (pre_visit, post_visit) = attributes.visit(quote!(value));
                        quote_spanned!(f.span() =>
                            if let Some(value) = &#modifier self.#name {
                                #pre_visit sqlparser::ast::#visit_trait::#visit_method(value, visitor)?; #post_visit
                            }
                        )
                    } else {
                        let (pre_visit, post_visit) = attributes.visit(quote!(&#modifier self.#name));
                        quote_spanned!(f.span() =>
                            #pre_visit sqlparser::ast::#visit_trait::#visit_method(&#modifier self.#name, visitor)?; #post_visit
                        )
                    }
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unnamed(fields) => {
                let recurse = fields.unnamed.iter().enumerate().map(|(i, f)| {
                    let index = Index::from(i);
                    let attributes = Attributes::parse(&f.attrs);
                    let (pre_visit, post_visit) = attributes.visit(quote!(&self.#index));
                    quote_spanned!(f.span() => #pre_visit sqlparser::ast::#visit_trait::#visit_method(&#modifier self.#index, visitor)?; #post_visit)
                });
                quote! {
                    #(#recurse)*
                }
            }
            Fields::Unit => {
                quote!()
            }
        },
        Data::Enum(data) => {
            let statements = data.variants.iter().map(|v| {
                let name = &v.ident;
                match &v.fields {
                    Fields::Named(fields) => {
                        let names = fields.named.iter().map(|f| &f.ident);
                        let visit = fields.named.iter().map(|f| {
                            let name = &f.ident;
                            let attributes = Attributes::parse(&f.attrs);
                            let (pre_visit, post_visit) = attributes.visit(name.to_token_stream());
                            quote_spanned!(f.span() => #pre_visit sqlparser::ast::#visit_trait::#visit_method(#name, visitor)?; #post_visit)
                        });

                        quote!(
                            Self::#name { #(#names),* } => {
                                #(#visit)*
                            }
                        )
                    }
                    Fields::Unnamed(fields) => {
                        let names = fields.unnamed.iter().enumerate().map(|(i, f)| format_ident!("_{}", i, span = f.span()));
                        let visit = fields.unnamed.iter().enumerate().map(|(i, f)| {
                            let name = format_ident!("_{}", i);
                            let attributes = Attributes::parse(&f.attrs);
                            let (pre_visit, post_visit) = attributes.visit(name.to_token_stream());
                            quote_spanned!(f.span() => #pre_visit sqlparser::ast::#visit_trait::#visit_method(#name, visitor)?; #post_visit)
                        });

                        quote! {
                            Self::#name ( #(#names),*) => {
                                #(#visit)*
                            }
                        }
                    }
                    Fields::Unit => {
                        quote! {
                            Self::#name => {}
                        }
                    }
                }
            });

            quote! {
                match self {
                    #(#statements),*
                }
            }
        }
        Data::Union(_) => unimplemented!(),
    }
}

fn is_option(ty: &Type) -> bool {
    if let Type::Path(TypePath {
        path: Path { segments, .. },
        ..
    }) = ty
    {
        if let Some(segment) = segments.last() {
            if segment.ident == "Option" {
                if let PathArguments::AngleBracketed(args) = &segment.arguments {
                    return args.args.len() == 1;
                }
            }
        }
    }
    false
}
