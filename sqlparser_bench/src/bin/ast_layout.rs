// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.

use std::mem::{align_of, size_of};

use sqlparser::ast::{
    AstBox, AttachedToken, DataType, Expr, Function, Join, Query, Select, SetExpr, Statement,
    TableFactor, Value,
};
use sqlparser::tokenizer::{Span, TokenWithSpan};

fn main() {
    println!("type                 size align boxed");
    println!("-------------------- ---- ----- -----");
    report::<Statement>("Statement");
    report::<Expr>("Expr");
    report::<Query>("Query");
    report::<Select>("Select");
    report::<SetExpr>("SetExpr");
    report::<TableFactor>("TableFactor");
    report::<Join>("Join");
    report::<Function>("Function");
    report::<DataType>("DataType");
    report::<Value>("Value");
    report::<AttachedToken>("AttachedToken");
    report::<Span>("Span");
    report::<TokenWithSpan<'static>>("TokenWithSpan");
}

fn report<T>(name: &str) {
    println!(
        "{name:<20} {size:>4} {align:>5} {boxed:>5}",
        size = size_of::<T>(),
        align = align_of::<T>(),
        boxed = size_of::<AstBox<T>>(),
    );
}
