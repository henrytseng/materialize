// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! `EXPLAIN` support for LIR structures.

pub(crate) mod json;
pub(crate) mod text;

use std::collections::HashMap;

use mz_compute_client::command::DataflowDescription;
use mz_compute_client::plan::Plan;
use mz_repr::explain_new::{Explain, ExplainConfig, ExplainError, UnsupportedFormat};

use super::common::{Explanation, JsonViewFormatter};
use super::{AnnotatedPlan, ExplainContext, ExplainMultiPlan, Explainable};

impl<'a> Explain<'a> for Explainable<'a, DataflowDescription<Plan>> {
    type Context = ExplainContext<'a>;

    type Text = ExplainMultiPlan<'a, Plan>;

    type Json = Explanation<'a, JsonViewFormatter, Plan>;

    type Dot = UnsupportedFormat;

    fn explain_text(
        &'a mut self,
        _config: &'a ExplainConfig,
        context: &'a Self::Context,
    ) -> Result<Self::Text, ExplainError> {
        let plans = self
            .0
            .objects_to_build
            .iter_mut()
            .rev()
            .map(|build_desc| {
                let id = context
                    .humanizer
                    .humanize_id(build_desc.id)
                    .unwrap_or_else(|| build_desc.id.to_string());
                let plan = AnnotatedPlan {
                    plan: &build_desc.plan,
                    annotations: HashMap::default(),
                };
                (id, plan)
            })
            .collect::<Vec<_>>();

        let sources = self
            .0
            .source_imports
            .iter_mut()
            .filter_map(|(id, (source_desc, _))| {
                source_desc.arguments.operators.as_ref().map(|op| {
                    let id = context
                        .humanizer
                        .humanize_id(*id)
                        .unwrap_or_else(|| id.to_string());
                    (id, op)
                })
            })
            .collect::<Vec<_>>();

        Ok(ExplainMultiPlan {
            context,
            sources,
            plans,
        })
    }

    fn explain_json(
        &'a mut self,
        _config: &'a ExplainConfig,
        context: &'a Self::Context,
    ) -> Result<Self::Json, ExplainError> {
        let formatter = JsonViewFormatter {};
        let mut explanation = Explanation::new_from_dataflow(self.0, context.humanizer, formatter);
        if let Some(row_set_finishing) = context.finishing.clone() {
            explanation.explain_row_set_finishing(row_set_finishing);
        }
        Ok(explanation)
    }
}
