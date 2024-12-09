#![allow(clippy::upper_case_acronyms)]

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize, Serializer};
use std::{collections::HashMap, convert::From};

pub type ValueAmount = Decimal;
pub type ClientIdentifier = u16;
pub type TransactionIdentifier = u32;

#[derive(Copy, Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum TransactionType {
    DEPOSIT,
    WITHDRAWAL,
    CHARGEBACK,
    DISPUTE,
    RESOLVE,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
pub struct Transaction {
    #[serde(rename = "type")]
    pub transaction_type: TransactionType,
    #[serde(rename = "client")]
    pub client_id: ClientIdentifier,
    #[serde(rename = "tx")]
    pub transaction_id: TransactionIdentifier,
    #[serde(rename = "amount", serialize_with = "serialize_value_amount_option")]
    pub transaction_amount: Option<ValueAmount>,
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(from = "Account")]
pub struct AccountView {
    #[serde(rename = "client")]
    pub client_id: ClientIdentifier,
    #[serde(serialize_with = "serialize_value_amount")]
    pub available: ValueAmount,
    #[serde(serialize_with = "serialize_value_amount")]
    pub held: ValueAmount,
    pub locked: bool,
    #[serde(serialize_with = "serialize_value_amount")]
    pub total: ValueAmount,
}

// Account to be used for all internal representations of account information
// The `AccountView` struct is only used when "rendering" an output which will include
// calculable values. Initial use-case is for the `total` field, which is calculated by
// adding `available` and `held`.
#[derive(Clone, Debug, Deserialize)]
pub struct Account {
    pub client_id: ClientIdentifier,
    pub available: ValueAmount,
    pub held: ValueAmount,
    pub locked: bool,
    pub settled_transactions_log: HashMap<TransactionIdentifier, Transaction>,
    pub disputed_transactions_log: HashMap<TransactionIdentifier, Transaction>,
}

impl From<Account> for AccountView {
    fn from(value: Account) -> Self {
        AccountView {
            client_id: value.client_id,
            available: value.available,
            held: value.held,
            total: value.available + value.held,
            locked: value.locked,
        }
    }
}

pub mod errors {
    #[derive(thiserror::Error, Debug, PartialEq)]
    pub enum ApplicationError {
        #[error("{0}")]
        FileAccess(String),

        #[error("{0}")]
        InvalidData(String),

        #[error("{0}")]
        CSV(String),
    }
}

/// Serialize value amount to a string with a consistent number of decimal places
fn serialize_value_amount<S>(val: &ValueAmount, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&format!("{:.4}", val))
}

/// Serialize value amount to a string with a consistent number of decimal places
fn serialize_value_amount_option<S>(
    val: &Option<ValueAmount>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match val {
        Some(value_amount) => serializer.serialize_str(&format!("{:.4}", value_amount)),
        None => serializer.serialize_none(),
    }
}
