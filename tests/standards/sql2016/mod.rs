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

//! SQL:2016 Standards Compliance Tests
//!
//! This module tracks coverage of SQL:2016 features defined in ISO/IEC 9075:2016.
//!
//! ## Coverage Summary
//!
//! | Series | Range | Description | Status |
//! |--------|-------|-------------|--------|
//! | E | E011-E182 | Core SQL features | In Progress |
//! | F | F021-F869 | Optional features | In Progress |
//! | T | T031-T670 | Advanced features | In Progress |
//! | S | S071-S404 | Array support | In Progress |
//! | X | X010-X400 | XML support | In Progress |
//! | R | R010-R030 | Row pattern recognition | In Progress |
//! | B | B200 | Polymorphic table functions | In Progress |

pub mod foundation;
pub mod pattern_recognition;
pub mod ptf;
