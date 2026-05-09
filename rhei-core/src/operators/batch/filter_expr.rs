//! Arrow kernel-based filter expression operator.
//!
//! Provides a predicate DSL (`col`, `lit`, comparisons, boolean logic) that
//! evaluates directly against Arrow arrays using `arrow::compute` kernels.
//! The result is a `BooleanArray` applied as a selection vector — zero-copy.

use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int64Type, UInt64Type};
use arrow_array::{Array, BooleanArray, RecordBatch, StringArray};
use async_trait::async_trait;

use crate::arrow::{BatchStreamFunction, BufferOutput, OperatorContext, RheiBuffer, RheiSchema};

/// A scalar value for comparison predicates.
#[derive(Clone, Debug)]
pub enum ScalarValue {
    /// 64-bit signed integer.
    Int64(i64),
    /// 64-bit unsigned integer.
    UInt64(u64),
    /// 64-bit floating point.
    Float64(f64),
    /// UTF-8 string.
    Utf8(String),
    /// Boolean.
    Bool(bool),
}

/// A predicate expression that evaluates against a `RecordBatch`.
#[derive(Clone, Debug)]
pub enum Expr {
    /// Reference to a named column.
    Column(String),
    /// A literal scalar value.
    Literal(ScalarValue),
    /// Greater than comparison.
    Gt(Box<Expr>, Box<Expr>),
    /// Greater than or equal comparison.
    GtEq(Box<Expr>, Box<Expr>),
    /// Less than comparison.
    Lt(Box<Expr>, Box<Expr>),
    /// Less than or equal comparison.
    LtEq(Box<Expr>, Box<Expr>),
    /// Equality comparison.
    Eq(Box<Expr>, Box<Expr>),
    /// Inequality comparison.
    NotEq(Box<Expr>, Box<Expr>),
    /// Logical AND of two boolean expressions.
    And(Box<Expr>, Box<Expr>),
    /// Logical OR of two boolean expressions.
    Or(Box<Expr>, Box<Expr>),
    /// Logical NOT of a boolean expression.
    Not(Box<Expr>),
}

/// Creates a column reference expression.
pub fn col(name: impl Into<String>) -> Expr {
    Expr::Column(name.into())
}

/// Creates a literal i64 expression.
pub fn lit_i64(v: i64) -> Expr {
    Expr::Literal(ScalarValue::Int64(v))
}

/// Creates a literal u64 expression.
pub fn lit_u64(v: u64) -> Expr {
    Expr::Literal(ScalarValue::UInt64(v))
}

/// Creates a literal f64 expression.
pub fn lit_f64(v: f64) -> Expr {
    Expr::Literal(ScalarValue::Float64(v))
}

/// Creates a literal string expression.
pub fn lit_str(v: impl Into<String>) -> Expr {
    Expr::Literal(ScalarValue::Utf8(v.into()))
}

/// Creates a literal bool expression.
pub fn lit_bool(v: bool) -> Expr {
    Expr::Literal(ScalarValue::Bool(v))
}

impl Expr {
    /// Greater than comparison.
    pub fn gt(self, other: Expr) -> Expr {
        Expr::Gt(Box::new(self), Box::new(other))
    }

    /// Greater than or equal comparison.
    pub fn gt_eq(self, other: Expr) -> Expr {
        Expr::GtEq(Box::new(self), Box::new(other))
    }

    /// Less than comparison.
    pub fn lt(self, other: Expr) -> Expr {
        Expr::Lt(Box::new(self), Box::new(other))
    }

    /// Less than or equal comparison.
    pub fn lt_eq(self, other: Expr) -> Expr {
        Expr::LtEq(Box::new(self), Box::new(other))
    }

    /// Equality comparison.
    pub fn eq(self, other: Expr) -> Expr {
        Expr::Eq(Box::new(self), Box::new(other))
    }

    /// Inequality comparison.
    pub fn not_eq(self, other: Expr) -> Expr {
        Expr::NotEq(Box::new(self), Box::new(other))
    }

    /// Logical AND.
    pub fn and(self, other: Expr) -> Expr {
        Expr::And(Box::new(self), Box::new(other))
    }

    /// Logical OR.
    pub fn or(self, other: Expr) -> Expr {
        Expr::Or(Box::new(self), Box::new(other))
    }

    /// Logical NOT.
    #[allow(clippy::should_implement_trait)]
    pub fn negate(self) -> Expr {
        Expr::Not(Box::new(self))
    }
}

/// Internal enum for intermediate evaluation results.
enum EvalResult {
    Array(Arc<dyn Array>),
    Scalar(ScalarValue),
}

fn evaluate(expr: &Expr, batch: &RecordBatch) -> anyhow::Result<EvalResult> {
    match expr {
        Expr::Column(name) => {
            let idx = batch.schema().index_of(name)?;
            Ok(EvalResult::Array(Arc::clone(batch.column(idx))))
        }
        Expr::Literal(v) => Ok(EvalResult::Scalar(v.clone())),
        Expr::Gt(lhs, rhs) => eval_cmp(lhs, rhs, batch, CmpOp::Gt),
        Expr::GtEq(lhs, rhs) => eval_cmp(lhs, rhs, batch, CmpOp::GtEq),
        Expr::Lt(lhs, rhs) => eval_cmp(lhs, rhs, batch, CmpOp::Lt),
        Expr::LtEq(lhs, rhs) => eval_cmp(lhs, rhs, batch, CmpOp::LtEq),
        Expr::Eq(lhs, rhs) => eval_cmp(lhs, rhs, batch, CmpOp::Eq),
        Expr::NotEq(lhs, rhs) => eval_cmp(lhs, rhs, batch, CmpOp::NotEq),
        Expr::And(lhs, rhs) => {
            let l = evaluate_to_bool(lhs, batch)?;
            let r = evaluate_to_bool(rhs, batch)?;
            Ok(EvalResult::Array(Arc::new(
                arrow::compute::kernels::boolean::and(&l, &r)?,
            )))
        }
        Expr::Or(lhs, rhs) => {
            let l = evaluate_to_bool(lhs, batch)?;
            let r = evaluate_to_bool(rhs, batch)?;
            Ok(EvalResult::Array(Arc::new(
                arrow::compute::kernels::boolean::or(&l, &r)?,
            )))
        }
        Expr::Not(inner) => {
            let b = evaluate_to_bool(inner, batch)?;
            Ok(EvalResult::Array(Arc::new(
                arrow::compute::kernels::boolean::not(&b)?,
            )))
        }
    }
}

fn evaluate_to_bool(expr: &Expr, batch: &RecordBatch) -> anyhow::Result<BooleanArray> {
    let result = evaluate(expr, batch)?;
    match result {
        EvalResult::Array(arr) => arr
            .as_any()
            .downcast_ref::<BooleanArray>()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("expression did not produce BooleanArray")),
        EvalResult::Scalar(ScalarValue::Bool(v)) => {
            Ok(BooleanArray::from(vec![v; batch.num_rows()]))
        }
        EvalResult::Scalar(_) => Err(anyhow::anyhow!("expected boolean expression")),
    }
}

#[derive(Clone, Copy)]
enum CmpOp {
    Gt,
    GtEq,
    Lt,
    LtEq,
    Eq,
    NotEq,
}

fn eval_cmp(lhs: &Expr, rhs: &Expr, batch: &RecordBatch, op: CmpOp) -> anyhow::Result<EvalResult> {
    let left = evaluate(lhs, batch)?;
    let right = evaluate(rhs, batch)?;

    let result = match (left, right) {
        (EvalResult::Array(arr), EvalResult::Scalar(scalar)) => {
            cmp_array_scalar(&arr, &scalar, op)?
        }
        (EvalResult::Scalar(scalar), EvalResult::Array(arr)) => {
            cmp_array_scalar(&arr, &scalar, flip_op(op))?
        }
        (EvalResult::Array(l), EvalResult::Array(r)) => cmp_array_array(&l, &r, op)?,
        (EvalResult::Scalar(_), EvalResult::Scalar(_)) => {
            anyhow::bail!("cannot compare two literals")
        }
    };
    Ok(EvalResult::Array(Arc::new(result)))
}

fn flip_op(op: CmpOp) -> CmpOp {
    match op {
        CmpOp::Gt => CmpOp::Lt,
        CmpOp::GtEq => CmpOp::LtEq,
        CmpOp::Lt => CmpOp::Gt,
        CmpOp::LtEq => CmpOp::GtEq,
        CmpOp::Eq => CmpOp::Eq,
        CmpOp::NotEq => CmpOp::NotEq,
    }
}

fn cmp_array_scalar(
    arr: &Arc<dyn Array>,
    scalar: &ScalarValue,
    op: CmpOp,
) -> anyhow::Result<BooleanArray> {
    use arrow::compute::kernels::cmp;

    match scalar {
        ScalarValue::Int64(v) => {
            let col = arr.as_primitive::<Int64Type>();
            let scalar_arr = arrow_array::Int64Array::new_scalar(*v);
            Ok(match op {
                CmpOp::Gt => cmp::gt(col, &scalar_arr)?,
                CmpOp::GtEq => cmp::gt_eq(col, &scalar_arr)?,
                CmpOp::Lt => cmp::lt(col, &scalar_arr)?,
                CmpOp::LtEq => cmp::lt_eq(col, &scalar_arr)?,
                CmpOp::Eq => cmp::eq(col, &scalar_arr)?,
                CmpOp::NotEq => cmp::neq(col, &scalar_arr)?,
            })
        }
        ScalarValue::UInt64(v) => {
            let col = arr.as_primitive::<UInt64Type>();
            let scalar_arr = arrow_array::UInt64Array::new_scalar(*v);
            Ok(match op {
                CmpOp::Gt => cmp::gt(col, &scalar_arr)?,
                CmpOp::GtEq => cmp::gt_eq(col, &scalar_arr)?,
                CmpOp::Lt => cmp::lt(col, &scalar_arr)?,
                CmpOp::LtEq => cmp::lt_eq(col, &scalar_arr)?,
                CmpOp::Eq => cmp::eq(col, &scalar_arr)?,
                CmpOp::NotEq => cmp::neq(col, &scalar_arr)?,
            })
        }
        ScalarValue::Float64(v) => {
            let col = arr.as_primitive::<Float64Type>();
            let scalar_arr = arrow_array::Float64Array::new_scalar(*v);
            Ok(match op {
                CmpOp::Gt => cmp::gt(col, &scalar_arr)?,
                CmpOp::GtEq => cmp::gt_eq(col, &scalar_arr)?,
                CmpOp::Lt => cmp::lt(col, &scalar_arr)?,
                CmpOp::LtEq => cmp::lt_eq(col, &scalar_arr)?,
                CmpOp::Eq => cmp::eq(col, &scalar_arr)?,
                CmpOp::NotEq => cmp::neq(col, &scalar_arr)?,
            })
        }
        ScalarValue::Utf8(v) => {
            let col = arr
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("expected string array"))?;
            let scalar_arr = arrow_array::StringArray::new_scalar(v);
            Ok(match op {
                CmpOp::Gt => cmp::gt(col, &scalar_arr)?,
                CmpOp::GtEq => cmp::gt_eq(col, &scalar_arr)?,
                CmpOp::Lt => cmp::lt(col, &scalar_arr)?,
                CmpOp::LtEq => cmp::lt_eq(col, &scalar_arr)?,
                CmpOp::Eq => cmp::eq(col, &scalar_arr)?,
                CmpOp::NotEq => cmp::neq(col, &scalar_arr)?,
            })
        }
        ScalarValue::Bool(v) => {
            let col = arr
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| anyhow::anyhow!("expected boolean array"))?;
            let scalar_arr = BooleanArray::new_scalar(*v);
            Ok(match op {
                CmpOp::Gt => cmp::gt(col, &scalar_arr)?,
                CmpOp::GtEq => cmp::gt_eq(col, &scalar_arr)?,
                CmpOp::Lt => cmp::lt(col, &scalar_arr)?,
                CmpOp::LtEq => cmp::lt_eq(col, &scalar_arr)?,
                CmpOp::Eq => cmp::eq(col, &scalar_arr)?,
                CmpOp::NotEq => cmp::neq(col, &scalar_arr)?,
            })
        }
    }
}

fn cmp_array_array(
    left: &Arc<dyn Array>,
    right: &Arc<dyn Array>,
    op: CmpOp,
) -> anyhow::Result<BooleanArray> {
    use arrow::compute::kernels::cmp;
    Ok(match op {
        CmpOp::Gt => cmp::gt(left, right)?,
        CmpOp::GtEq => cmp::gt_eq(left, right)?,
        CmpOp::Lt => cmp::lt(left, right)?,
        CmpOp::LtEq => cmp::lt_eq(left, right)?,
        CmpOp::Eq => cmp::eq(left, right)?,
        CmpOp::NotEq => cmp::neq(left, right)?,
    })
}

/// Evaluates an [`Expr`] predicate against a `RecordBatch` and returns a `BooleanArray`.
pub fn eval_predicate(expr: &Expr, batch: &RecordBatch) -> anyhow::Result<BooleanArray> {
    evaluate_to_bool(expr, batch)
}

/// Zero-copy filter operator using arrow compute kernel predicates.
///
/// Evaluates an `Expr` against the input batch columns, producing a
/// `BooleanArray` mask that is composed with any existing selection vector.
pub struct BatchFilterExprOp<T> {
    expr: Expr,
    _phantom: PhantomData<fn(T)>,
}

impl<T> fmt::Debug for BatchFilterExprOp<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BatchFilterExprOp")
            .field("expr", &self.expr)
            .finish_non_exhaustive()
    }
}

impl<T> Clone for BatchFilterExprOp<T> {
    fn clone(&self) -> Self {
        Self {
            expr: self.expr.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<T: RheiSchema> BatchFilterExprOp<T> {
    /// Creates a new filter operator with the given predicate expression.
    pub fn new(expr: Expr) -> Self {
        Self {
            expr,
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<T: RheiSchema> BatchStreamFunction for BatchFilterExprOp<T> {
    type Input = T;
    type Output = T;

    async fn process(
        &mut self,
        input: RheiBuffer<T>,
        _ctx: &mut OperatorContext,
    ) -> anyhow::Result<BufferOutput<T>> {
        if input.physical_len() == 0 {
            return Ok(BufferOutput::None);
        }

        let batch = input.as_record_batch();
        let mask = eval_predicate(&self.expr, batch)?;
        let filtered = input.and_mask(mask);

        if filtered.is_empty() {
            return Ok(BufferOutput::None);
        }
        Ok(BufferOutput::Single(filtered))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use arrow_array::builder::ArrayBuilder;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    use crate::arrow::RheiSchema as RheiSchemaTrait;
    use crate::arrow::{OperatorContext, RheiBuffer, RheiBuilder};
    use crate::state::context::StateContext;
    use crate::state::local_backend::LocalBackend;

    struct Sensor {
        name: String,
        value: f64,
        ts: u64,
    }

    struct SensorBuilder {
        name: arrow_array::builder::StringBuilder,
        value: arrow_array::builder::PrimitiveBuilder<Float64Type>,
        ts: arrow_array::builder::PrimitiveBuilder<UInt64Type>,
    }

    struct SensorView<'a> {
        #[allow(dead_code)]
        name: &'a str,
        value: f64,
        #[allow(dead_code)]
        ts: u64,
    }

    struct SensorCols<'a> {
        #[allow(dead_code)]
        name: &'a StringArray,
        #[allow(dead_code)]
        value: &'a arrow_array::PrimitiveArray<Float64Type>,
        #[allow(dead_code)]
        ts: &'a arrow_array::PrimitiveArray<UInt64Type>,
    }

    impl crate::arrow::RheiBuilder for SensorBuilder {
        type Item = Sensor;

        fn append(&mut self, item: Sensor) {
            self.name.append_value(&item.name);
            self.value.append_value(item.value);
            self.ts.append_value(item.ts);
        }

        fn append_null(&mut self) {
            self.name.append_null();
            self.value.append_null();
            self.ts.append_null();
        }

        fn len(&self) -> usize {
            self.name.len()
        }

        fn finish(mut self) -> RecordBatch {
            RecordBatch::try_new(
                Sensor::arrow_schema(),
                vec![
                    Arc::new(self.name.finish()),
                    Arc::new(self.value.finish()),
                    Arc::new(self.ts.finish()),
                ],
            )
            .unwrap()
        }
    }

    impl RheiSchemaTrait for Sensor {
        type Builder = SensorBuilder;
        type View<'a> = SensorView<'a>;
        type Columns<'a> = SensorCols<'a>;

        fn arrow_schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![
                Field::new("name", DataType::Utf8, false),
                Field::new("value", DataType::Float64, false),
                Field::new("ts", DataType::UInt64, false),
            ]))
        }

        fn builder(capacity: usize) -> Self::Builder {
            SensorBuilder {
                name: arrow_array::builder::StringBuilder::with_capacity(capacity, capacity * 8),
                value: arrow_array::builder::PrimitiveBuilder::with_capacity(capacity),
                ts: arrow_array::builder::PrimitiveBuilder::with_capacity(capacity),
            }
        }

        fn view(batch: &RecordBatch, index: usize) -> Self::View<'_> {
            SensorView {
                name: batch.column(0).as_string::<i32>().value(index),
                value: batch.column(1).as_primitive::<Float64Type>().value(index),
                ts: batch.column(2).as_primitive::<UInt64Type>().value(index),
            }
        }

        fn columns(batch: &RecordBatch) -> Self::Columns<'_> {
            SensorCols {
                name: batch.column(0).as_string::<i32>(),
                value: batch.column(1).as_primitive::<Float64Type>(),
                ts: batch.column(2).as_primitive::<UInt64Type>(),
            }
        }
    }

    fn test_ctx() -> OperatorContext {
        let dir = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(dir.path().join("state.json"), None).unwrap();
        OperatorContext::new(StateContext::new(Box::new(backend)))
    }

    fn make_sensors() -> RheiBuffer<Sensor> {
        let mut builder = Sensor::builder(5);
        builder.append(Sensor {
            name: "a".into(),
            value: 10.0,
            ts: 1,
        });
        builder.append(Sensor {
            name: "b".into(),
            value: 25.0,
            ts: 2,
        });
        builder.append(Sensor {
            name: "a".into(),
            value: 30.0,
            ts: 3,
        });
        builder.append(Sensor {
            name: "c".into(),
            value: 5.0,
            ts: 4,
        });
        builder.append(Sensor {
            name: "b".into(),
            value: 50.0,
            ts: 5,
        });
        RheiBuffer::from_builder(builder)
    }

    #[tokio::test]
    async fn filter_gt_scalar() {
        let mut ctx = test_ctx();
        let mut op = BatchFilterExprOp::<Sensor>::new(col("value").gt(lit_f64(20.0)));

        let input = make_sensors();
        let result = op.process(input, &mut ctx).await.unwrap();
        let BufferOutput::Single(buf) = result else {
            panic!("expected Single");
        };
        assert_eq!(buf.len(), 3);
        let values: Vec<f64> = buf.iter().map(|v| v.value).collect();
        assert_eq!(values, vec![25.0, 30.0, 50.0]);
    }

    #[tokio::test]
    async fn filter_eq_string() {
        let mut ctx = test_ctx();
        let mut op = BatchFilterExprOp::<Sensor>::new(col("name").eq(lit_str("a")));

        let input = make_sensors();
        let result = op.process(input, &mut ctx).await.unwrap();
        let BufferOutput::Single(buf) = result else {
            panic!("expected Single");
        };
        assert_eq!(buf.len(), 2);
    }

    #[tokio::test]
    async fn filter_and_compound() {
        let mut ctx = test_ctx();
        let expr = col("value").gt(lit_f64(10.0)).and(col("ts").lt(lit_u64(5)));
        let mut op = BatchFilterExprOp::<Sensor>::new(expr);

        let input = make_sensors();
        let result = op.process(input, &mut ctx).await.unwrap();
        let BufferOutput::Single(buf) = result else {
            panic!("expected Single");
        };
        // value > 10 AND ts < 5 → (25.0, ts=2), (30.0, ts=3)
        assert_eq!(buf.len(), 2);
    }

    #[tokio::test]
    async fn filter_or() {
        let mut ctx = test_ctx();
        let expr = col("value")
            .lt(lit_f64(10.0))
            .or(col("value").gt(lit_f64(40.0)));
        let mut op = BatchFilterExprOp::<Sensor>::new(expr);

        let input = make_sensors();
        let result = op.process(input, &mut ctx).await.unwrap();
        let BufferOutput::Single(buf) = result else {
            panic!("expected Single");
        };
        // value < 10 → (5.0), value > 40 → (50.0)
        assert_eq!(buf.len(), 2);
    }

    #[tokio::test]
    async fn filter_not() {
        let mut ctx = test_ctx();
        let mut op = BatchFilterExprOp::<Sensor>::new(col("name").eq(lit_str("a")).negate());

        let input = make_sensors();
        let result = op.process(input, &mut ctx).await.unwrap();
        let BufferOutput::Single(buf) = result else {
            panic!("expected Single");
        };
        // not(name == "a") → 3 rows (b, c, b)
        assert_eq!(buf.len(), 3);
    }

    #[tokio::test]
    async fn filter_all_excluded_returns_none() {
        let mut ctx = test_ctx();
        let mut op = BatchFilterExprOp::<Sensor>::new(col("value").gt(lit_f64(1000.0)));

        let input = make_sensors();
        let result = op.process(input, &mut ctx).await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn filter_empty_input() {
        let mut ctx = test_ctx();
        let mut op = BatchFilterExprOp::<Sensor>::new(col("value").gt(lit_f64(0.0)));

        let input: RheiBuffer<Sensor> = RheiBuffer::from_builder(Sensor::builder(0));
        let result = op.process(input, &mut ctx).await.unwrap();
        assert!(result.is_empty());
    }
}
