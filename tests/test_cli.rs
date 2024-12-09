use assert_cmd::Command;
use chrono::Utc;
use predicates::prelude::*;
use std::{error::Error, fs};

use transaction_engine::engine::MB_THRESHOLD;

#[test]
fn test_basic_transactions() -> Result<(), Box<dyn Error>> {
    let input_file = "data/tests/transaction_batch_single_account.csv";

    let expected_output = "\
        client,available,held,locked,total\n\
        5,435930.1231,0.0000,false,435930.1231";

    Command::cargo_bin("transaction-engine")?
        .arg(input_file)
        .assert()
        .success()
        .stdout(predicate::str::contains(expected_output));

    Ok(())
}

#[test]
fn test_validation() -> Result<(), Box<dyn Error>> {
    let terminal_output = Command::cargo_bin("generate-test-data")?
        .arg("100000")
        .assert()
        .success()
        .get_output()
        .clone();

    let now_timestamp = Utc::now().format("%Y%m%d%H%M%S");
    let generated_input_filename = format!("data/tests/integ_test_{}.csv", now_timestamp);

    let stdout_csv_str = String::from_utf8_lossy(&terminal_output.stdout);

    // Now save this output to a CSV file within the tests directory
    // For instance, tests/output.csv
    fs::write(&generated_input_filename, stdout_csv_str.as_ref())?;

    // Test max file size validation
    let terminal_output = Command::cargo_bin("transaction-engine")?
        .arg(&generated_input_filename)
        .assert()
        .failure()
        .get_output()
        .clone();
    let error_response_msg = String::from_utf8_lossy(&terminal_output.stderr);

    println!("Error response message: {}", error_response_msg);
    assert!(error_response_msg.contains("Error: InvalidData"));
    assert!(
        error_response_msg.contains(format!("Data file [{}]", generated_input_filename).as_str())
    );
    assert!(error_response_msg
        .contains(format!("exceeds input limit of {} megabytes", MB_THRESHOLD).as_str()));

    fs::remove_file(&generated_input_filename)?;
    Ok(())
}

#[test]
fn test_generator_tool_execution() -> Result<(), Box<dyn Error>> {
    Command::cargo_bin("generate-test-data")?
        .arg("10")
        .assert()
        .success();

    Ok(())
}
