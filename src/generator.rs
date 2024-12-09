#![allow(clippy::redundant_field_names)]
#![allow(clippy::upper_case_acronyms)]

mod types;

use clap::Parser;
use csv::Writer;
use rand::distributions::Standard;
use rand::prelude::ThreadRng;
use rand::seq::IteratorRandom;
use rand::{thread_rng, Rng};
use rust_decimal::Decimal;
use types::{ClientIdentifier, Transaction, TransactionIdentifier, TransactionType, ValueAmount};

/// Command line arguments
#[derive(Parser)]
#[command(version = "1.0", about = "Generates a CSV file with random records")]
struct CLI {
    /// Number of records to generate
    #[arg(value_parser = clap::value_parser!(u32).range(1..))]
    count: u32,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = CLI::parse();

    let mut wtr = Writer::from_writer(std::io::stdout());
    // Write header automatically when we serialize the first record
    // because we derived Serialize. The csv crate will handle headers if we use `WriterBuilder`
    // with `has_headers(true)`. By default, `has_headers(true)` is the default for `Writer`.

    // Only randomize the selection of types that aren't dependent on each other.
    //
    // TransactionType::RESOLVE is dependent on a prior DISPUTE transaction.
    // TransactionType::CHARGEBACK,
    let type_variants: [TransactionType; 3] = [
        TransactionType::DEPOSIT,
        TransactionType::WITHDRAWAL,
        TransactionType::DISPUTE,
    ];

    let mut rng: ThreadRng = thread_rng();

    for _ in 0..cli.count {
        let transaction_type: TransactionType = *type_variants.iter().choose(&mut rng).unwrap();
        let client_identifier: ClientIdentifier = rng.gen_range(1..30);
        let transaction_identifier: TransactionIdentifier = rng.sample(Standard);
        let mut transaction_amount: Option<ValueAmount> = None;
        if transaction_type == TransactionType::DEPOSIT
            || transaction_type == TransactionType::WITHDRAWAL
        {
            transaction_amount =
                Some(Decimal::from_f64_retain(rng.gen_range(10.0..1000000.0)).unwrap());
        }

        let record: Transaction = Transaction {
            transaction_type: transaction_type,
            client_id: client_identifier,
            transaction_id: transaction_identifier,
            transaction_amount: transaction_amount,
        };

        wtr.serialize(record)?;
    }

    wtr.flush()?;
    Ok(())
}
