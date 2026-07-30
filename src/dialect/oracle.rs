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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::dialect::Dialect;
use crate::dialect::Precedence;
use crate::keywords::{self, Keyword};
use crate::parser::{Parser, ParserError};
use crate::tokenizer::BorrowedToken;

#[cfg(not(feature = "std"))]
use alloc::{borrow::ToOwned, string::String};

/// A dialect for Oracle AI Database.
/// Oracle grammar extensions are represented directly in the parser and AST.
#[derive(Debug)]
pub struct OracleDialect {}

impl Dialect for OracleDialect {
    fn identifier_quote_style(&self, _identifier: &str) -> Option<char> {
        Some('"')
    }

    fn canonicalize_identifier(&self, identifier: &str, quote_style: Option<char>) -> String {
        if quote_style.is_none() {
            identifier.to_uppercase()
        } else {
            identifier.to_owned()
        }
    }

    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '"'
    }

    fn is_identifier_start(&self, ch: char) -> bool {
        ch.is_alphabetic()
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_alphanumeric() || matches!(ch, '_' | '$' | '#')
    }

    fn is_table_alias(&self, keyword: &Keyword, _parser: &Parser) -> bool {
        *keyword != Keyword::LOG && !keywords::RESERVED_FOR_TABLE_ALIAS.contains(keyword)
    }

    fn supports_group_by_expr(&self) -> bool {
        true
    }

    fn supports_outer_join_operator(&self) -> bool {
        true
    }

    fn supports_connect_by(&self) -> bool {
        true
    }

    fn supports_execute_immediate(&self) -> bool {
        true
    }

    fn supports_match_recognize(&self) -> bool {
        true
    }

    fn supports_comment_on(&self) -> bool {
        true
    }

    fn supports_table_sample_before_alias(&self) -> bool {
        true
    }

    fn supports_alternative_quoted_string_literal(&self) -> bool {
        true
    }

    fn supports_named_fn_args_with_rarrow_operator(&self) -> bool {
        true
    }

    fn supports_dollar_placeholder(&self) -> bool {
        true
    }

    fn get_next_precedence(&self, parser: &Parser) -> Option<Result<u8, ParserError>> {
        match (
            &parser.peek_token_ref().token,
            &parser.peek_nth_token_ref(1).token,
        ) {
            (BorrowedToken::Word(at), BorrowedToken::Word(local))
                if at.keyword == Keyword::AT && local.keyword == Keyword::LOCAL =>
            {
                Some(Ok(self.prec_value(Precedence::AtTz)))
            }
            (BorrowedToken::Word(word), _)
                if matches!(
                    word.value.to_ascii_uppercase().as_str(),
                    "LIKEC" | "LIKE2" | "LIKE4"
                ) =>
            {
                Some(Ok(self.prec_value(Precedence::Like)))
            }
            (BorrowedToken::Word(not), BorrowedToken::Word(word))
                if not.keyword == Keyword::NOT
                    && matches!(
                        word.value.to_ascii_uppercase().as_str(),
                        "LIKEC" | "LIKE2" | "LIKE4" | "MEMBER"
                    ) =>
            {
                Some(Ok(self.prec_value(Precedence::Like)))
            }
            _ => None,
        }
    }
}
