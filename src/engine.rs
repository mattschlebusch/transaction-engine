use std::{collections::HashMap, fs::File, io::BufReader, path::Path};

use csv::{ReaderBuilder, Writer};
use log::{debug, error, warn};
use rust_decimal_macros::dec;

use crate::types::{errors::ApplicationError, Account, AccountView, ClientIdentifier, Transaction};

// const ACCOUNT_DATA_PATH: &str = "data/snapshots/account_data_2024_01_01.csv";
pub const MB_THRESHOLD: u64 = 2;
const MAX_DATA_FILE_SIZE_MB: u64 = 1024 * 1024 * MB_THRESHOLD;

pub fn run_transactions(data_file_str: &str) -> Result<(), ApplicationError> {
    pre_validate_input_file(data_file_str)?;

    // Load transaction requests file
    let transaction_data: Vec<Transaction> = read_transaction_data(data_file_str)?;
    debug!("Read transaction data: \n{:?}", transaction_data);

    let mut account_data: HashMap<ClientIdentifier, Account> = HashMap::new();

    let _ = transaction_data
        .iter()
        .map(|transaction| process_transaction(&mut account_data, transaction))
        .collect::<Vec<_>>();

    // Output the results of the transaction
    debug!("Account data pre-publish: \n{:?}", account_data);
    publish(account_data.values().collect::<Vec<_>>())?;

    Ok(())
}

fn read_transaction_data(data_file_str: &str) -> Result<Vec<Transaction>, ApplicationError> {
    let mut transactions: Vec<Transaction> = Vec::new();

    let data_file: File = File::open(data_file_str).map_err(|io_err| {
        ApplicationError::FileAccess(format!(
            "Error reading batch data file [{:?}] - [{:?}]",
            data_file_str, io_err
        ))
    })?;

    let mut reader_builder = ReaderBuilder::new()
        .trim(csv::Trim::All)
        .has_headers(true)
        .from_reader(BufReader::new(data_file));

    for csv_result in reader_builder.deserialize() {
        match csv_result {
            Ok(transaction) => {
                debug!("Transaction read: \n{:?}", transaction);
                transactions.push(transaction);
            }
            Err(err) => error!("Error processing CSV record, skipping - {}", err),
        }
    }

    Ok(transactions)
}

fn process_transaction(
    account_data: &mut HashMap<ClientIdentifier, Account>,
    incoming_transaction: &Transaction,
) -> Result<(), ApplicationError> {
    debug!(
        "Process transaction: {}",
        incoming_transaction.transaction_id
    );
    let mut account: Account = match account_data.get(&incoming_transaction.client_id) {
        None => Account {
            available: dec!(0.0),
            client_id: incoming_transaction.client_id,
            held: dec!(0.0),
            locked: false,
            settled_transactions_log: HashMap::new(),
            disputed_transactions_log: HashMap::new(),
        },
        Some(account) => account.clone(),
    };
    debug!("Account data lookup: \n{:?}", account);

    // TODO Validate against repeated/duplicate transactions by transaction id
    // TODO Block accounts that are locked
    // TODO Introduce transaction to unlock accounts
    match incoming_transaction.transaction_type {
        crate::types::TransactionType::DEPOSIT => {
            match incoming_transaction.transaction_amount {
                Some(amount) => account.available += amount,
                None => return Err(ApplicationError::InvalidData(format!("Transaction id [{}] - Transaction amount value missing for deposit transaction type", incoming_transaction.transaction_id))),
            }
            account
                .settled_transactions_log
                .insert(incoming_transaction.transaction_id, *incoming_transaction);
        }
        crate::types::TransactionType::WITHDRAWAL => {
            // Deduct value from account
            match incoming_transaction.transaction_amount {
                Some(amount) => {
                    // If available funds are not sufficient, fail the transaction.
                    if account.available > amount {
                        account.available -= amount;
                    }
                },
                None => return Err(ApplicationError::InvalidData(format!("Transaction id [{}] - Transaction amount value missing for withdrawal transaction type", incoming_transaction.transaction_id))),
            }
            account
                .settled_transactions_log
                .insert(incoming_transaction.transaction_id, *incoming_transaction);
        }
        crate::types::TransactionType::CHARGEBACK => {
            // Like a RESOLVE transaction, is a subsequent transaction to a DISPUTE.
            // Locks the account

            // Move amount defined by transaction in question, from held back to available and
            // allocate the transaction back to the settled log.
            let dropped_transaction = account
                .disputed_transactions_log
                .remove(&incoming_transaction.transaction_id);
            match dropped_transaction {
                Some(transaction) => {
                    match transaction.transaction_amount {
                        Some(amount) => {
                            account.held -= amount;
                        },
                        None => error!("[{}] - Data corruption error - Dropped transaction missing value amount", transaction.transaction_id),
                    }
                },
                None => warn!("[{}] - Resolve transaction received but referenced an unsettled transaction not found for account [{}]", incoming_transaction.transaction_id, account.client_id),
            }
        }
        crate::types::TransactionType::DISPUTE => {
            // Move amount defined by transaction in question, from available to held and allocate
            // the transaction to the unsettled log.
            let unsettled_transaction = account
                .settled_transactions_log
                .remove(&incoming_transaction.transaction_id);
            match unsettled_transaction {
                Some(transaction) => {
                    match transaction.transaction_amount {
                        Some(amount) => {
                            account.disputed_transactions_log.insert(transaction.transaction_id, transaction);
                            account.available -= amount;
                            account.held += amount;
                        },
                        None => error!("[{}] - Data corruption error - Settled account transaction missing value amount", transaction.transaction_id),
                    }
                },
                None => warn!("[{}] - Dispute transaction received but referenced transaction not found for account [{}]", incoming_transaction.transaction_id, account.client_id),
            }
        }
        crate::types::TransactionType::RESOLVE => {
            // Move amount defined by transaction in question, from held back to available and
            // allocate the transaction back to the settled log.
            let resettled_transaction = account
                .disputed_transactions_log
                .remove(&incoming_transaction.transaction_id);
            match resettled_transaction {
                Some(transaction) => {
                    match transaction.transaction_amount {
                        Some(amount) => {
                            account.settled_transactions_log.insert(transaction.transaction_id, transaction);
                            account.available += amount;
                            account.held -= amount;
                        },
                        None => error!("[{}] - Data corruption error - Unsettled account transaction missing value amount", transaction.transaction_id),
                    }
                },
                None => warn!("[{}] - Resolve transaction received but referenced an unsettled transaction not found for account [{}]", incoming_transaction.transaction_id, account.client_id),
            }
        }
    }

    account_data.insert(account.client_id, account);

    Ok(())
}

fn publish(account_data: Vec<&Account>) -> Result<(), ApplicationError> {
    debug!("*****************************");
    debug!("Account data collection: \n{:?}", account_data);
    let mut csv_writer = Writer::from_writer(vec![]);
    let _ser_result = account_data
        .iter()
        .map(|account| {
            debug!("Account preserialized: \n{:?}", account);
            let account_view: AccountView = AccountView::from((**account).clone());
            debug!("Serialized Account View: \n{:?}", account_view);
            let _ = csv_writer.serialize(&account_view).map_err(|err| {
                error!(
                    "Error serializing account status [{:?}] - [{:?}]",
                    account_view.client_id.clone(),
                    err
                );
            });
        })
        .collect::<Vec<_>>();

    let csv_bytes = csv_writer.into_inner().map_err(|err| {
        ApplicationError::CSV(format!("Error serializing CSV data - [{:?}]", err))
    })?;
    let csv_data: String = String::from_utf8(csv_bytes).map_err(|err| {
        ApplicationError::CSV(format!("Error serializing CSV data - [{:?}]", err))
    })?;

    println!("{}", csv_data);

    Ok(())
}

/// Validate application argument/s
/// - Data file is accessible
/// - File size is under the maximum supported batch size
fn pre_validate_input_file(data_file_str: &str) -> Result<(), ApplicationError> {
    let file_path: &Path = Path::new(data_file_str);

    // Test accessibility
    let transaction_file = match File::open(file_path) {
        Err(err) => {
            panic!("Unable to open [{:?}] - {:?}", file_path, err);
        }
        Ok(file) => {
            debug!("File is open-able");
            file
        }
    };

    // Check the file size is under the supported maximum
    match transaction_file.metadata() {
        Err(err) => panic!(
            "Unable to read file metadata for file [{}].\n{}",
            data_file_str, err,
        ),
        Ok(metadata) => {
            // Only process transaction files smaller than the maximum threshold.
            if metadata.len() > MAX_DATA_FILE_SIZE_MB {
                return Err(ApplicationError::InvalidData(format!(
                    "Data file [{}] size of [{}] bytes which exceeds input limit of {} megabytes",
                    data_file_str,
                    metadata.len(),
                    MB_THRESHOLD,
                )));
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use rust_decimal_macros::dec;

    use crate::{
        engine::process_transaction,
        types::{Account, ClientIdentifier, Transaction, TransactionType},
    };

    #[test]
    fn test_deposit_withdrawal_transaction_success() {
        let account_data: &mut HashMap<ClientIdentifier, Account> = &mut HashMap::new();
        let transaction_result = process_transaction(
            account_data,
            &Transaction {
                client_id: 1,
                transaction_id: 1,
                transaction_type: TransactionType::DEPOSIT,
                transaction_amount: Some(dec!(100.0)),
            },
        );
        assert!(transaction_result.is_ok());
        assert_eq!(account_data.len(), 1);
        assert_eq!(account_data.get(&1).unwrap().available, dec!(100.0));

        let transaction_result = process_transaction(
            account_data,
            &Transaction {
                client_id: 1,
                transaction_id: 2,
                transaction_type: TransactionType::WITHDRAWAL,
                transaction_amount: Some(dec!(55.0)),
            },
        );
        assert!(transaction_result.is_ok());
        assert_eq!(account_data.len(), 1);
        assert_eq!(account_data.get(&1).unwrap().available, dec!(45.0));
        assert!(!account_data.get(&1).unwrap().locked);
        assert_eq!(account_data.get(&1).unwrap().client_id, 1);
    }

    #[test]
    fn test_dispute_chargeback_transaction_success() {
        let account_data: &mut HashMap<ClientIdentifier, Account> = &mut HashMap::new();
        let transaction_result = process_transaction(
            account_data,
            &Transaction {
                client_id: 1,
                transaction_id: 1,
                transaction_type: TransactionType::DEPOSIT,
                transaction_amount: Some(dec!(100.0)),
            },
        );
        assert!(transaction_result.is_ok());
        assert_eq!(account_data.get(&1).unwrap().available, dec!(100.0));

        let transaction_result = process_transaction(
            account_data,
            &Transaction {
                client_id: 1,
                transaction_id: 2,
                transaction_type: TransactionType::DEPOSIT,
                transaction_amount: Some(dec!(41.7)),
            },
        );
        assert!(transaction_result.is_ok());
        assert_eq!(account_data.get(&1).unwrap().available, dec!(141.7));

        // Dispute transaction 2
        let transaction_result = process_transaction(
            account_data,
            &Transaction {
                client_id: 1,
                transaction_id: 2,
                transaction_type: TransactionType::DISPUTE,
                transaction_amount: None,
            },
        );
        assert!(transaction_result.is_ok());
        assert_eq!(account_data.get(&1).unwrap().held, dec!(41.7));
        assert_eq!(account_data.get(&1).unwrap().available, dec!(100.0));

        // Resolve the dispute
        let transaction_result = process_transaction(
            account_data,
            &Transaction {
                client_id: 1,
                transaction_id: 2,
                transaction_type: TransactionType::CHARGEBACK,
                transaction_amount: None,
            },
        );
        assert!(transaction_result.is_ok());
        assert_eq!(account_data.get(&1).unwrap().held, dec!(0.0));
        assert_eq!(account_data.get(&1).unwrap().available, dec!(100.0));
    }

    #[test]
    fn test_dispute_resolve_transaction_success() {
        let account_data: &mut HashMap<ClientIdentifier, Account> = &mut HashMap::new();
        let transaction_result = process_transaction(
            account_data,
            &Transaction {
                client_id: 1,
                transaction_id: 1,
                transaction_type: TransactionType::DEPOSIT,
                transaction_amount: Some(dec!(100.0)),
            },
        );
        assert!(transaction_result.is_ok());
        assert_eq!(account_data.get(&1).unwrap().available, dec!(100.0));

        let transaction_result = process_transaction(
            account_data,
            &Transaction {
                client_id: 1,
                transaction_id: 2,
                transaction_type: TransactionType::DEPOSIT,
                transaction_amount: Some(dec!(31.5)),
            },
        );
        assert!(transaction_result.is_ok());
        assert_eq!(account_data.get(&1).unwrap().available, dec!(131.5));

        // Dispute transaction 2
        let transaction_result = process_transaction(
            account_data,
            &Transaction {
                client_id: 1,
                transaction_id: 2,
                transaction_type: TransactionType::DISPUTE,
                transaction_amount: None,
            },
        );
        assert!(transaction_result.is_ok());
        assert_eq!(account_data.get(&1).unwrap().held, dec!(31.5));
        assert_eq!(account_data.get(&1).unwrap().available, dec!(100.0));

        // Resolve the dispute
        let transaction_result = process_transaction(
            account_data,
            &Transaction {
                client_id: 1,
                transaction_id: 2,
                transaction_type: TransactionType::RESOLVE,
                transaction_amount: None,
            },
        );
        assert!(transaction_result.is_ok());
        assert_eq!(account_data.get(&1).unwrap().held, dec!(0.0));
        assert_eq!(account_data.get(&1).unwrap().available, dec!(131.5));
    }
}
