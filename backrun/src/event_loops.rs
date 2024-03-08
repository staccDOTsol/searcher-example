use futures_util::StreamExt;
use jito_protos::{bundle::BundleResult, searcher::{mempool_subscription, MempoolSubscription, PendingTxNotification}};
use jito_searcher_client::get_searcher_client;
use solana_sdk::{pubkey::Pubkey, signature::Keypair};
use yellowstone_grpc_client::{GeyserGrpcClient, GeyserGrpcClientError};
use yellowstone_grpc_proto::{geyser::subscribe_update::UpdateOneof, prelude::*};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::{sync::mpsc::Sender, time::sleep};
use log::{error, info};
use solana_metrics::{datapoint_error, datapoint_info};

pub async fn slot_subscribe_loop(endpoint: String, slot_sender: Sender<u64>) -> anyhow::Result<()> {
    let mut client = GeyserGrpcClient::connect_with_timeout(
        "https://jarrett-solana-7ba9.mainnet.rpcpool.com",
        Some("8d890735-edf2-4a75-af84-92f7c9e31718"), // Add your token if authentication is required
        None, // Additional headers can be added here
        Some(Duration::from_secs(10)), // Connection timeout
        Some(Duration::from_secs(10)), // Timeout for operations
        false, // Set to true if you want to skip certificate verification
    )
    .await?;

    let slots_filter = SubscribeRequestFilterSlots {
        filter_by_commitment: Some(true), // Adjust the filter based on your needs
    };

    let mut slots_map: HashMap<String, SubscribeRequestFilterSlots> = HashMap::new();
    slots_map.insert("client".to_owned(), slots_filter);

    let subscribe_request = SubscribeRequest {
        slots: slots_map,
        ..Default::default()
    };

    let (_, mut stream) = client.subscribe_with_request(Some(subscribe_request)).await?;

    while let Some(message) = stream.next().await {
        match message {
            Ok(update) => match update.update_oneof {
                Some(UpdateOneof::Slot(slot_update)) => {
                    if let Err(e) = slot_sender.send(slot_update.slot).await {
                        error!("Failed to send slot update: {:?}", e);
                        break;
                    }
                }
                _ => {}
            },
            Err(e) => {
                error!("Error receiving slot update: {:?}", e);
                break;
            }
        }
    }

    Ok(())
}

pub async fn block_subscribe_loop(endpoint: String, block_sender: Sender<SubscribeUpdateBlock>) -> anyhow::Result<()> {
    let mut client = GeyserGrpcClient::connect_with_timeout(
        "https://jarrett-solana-7ba9.mainnet.rpcpool.com",
        Some("8d890735-edf2-4a75-af84-92f7c9e31718"),
        None,
        Some(Duration::from_secs(10)),
        Some(Duration::from_secs(10)),
        false,
    )
    .await?;

    let blocks_filter = SubscribeRequestFilterBlocks {
        account_include: vec!["675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8".to_string()
        ,"CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK".to_string(),
        "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc".to_string()
        ], // Specify accounts to include if necessary
        include_transactions: Some(true), // Decide based on your needs
        include_accounts: Some(true),
        include_entries: Some(true),
    };

    let mut blocks_map: HashMap<String, SubscribeRequestFilterBlocks> = HashMap::new();
    blocks_map.insert("client".to_owned(), blocks_filter);

    let subscribe_request = SubscribeRequest {
        blocks: blocks_map,
        ..Default::default()
    };

    let (_, mut stream) = client.subscribe_with_request(Some(subscribe_request)).await?;

    while let Some(message) = stream.next().await {
        match message {
            Ok(update) => match update.update_oneof {
                Some(UpdateOneof::Block(block_update)) => {
                    if let Err(e) = block_sender.send(block_update).await {
                        error!("Failed to send block update: {:?}", e);
                        break;
                    }
                }
                _ => {}
            },
            Err(e) => {
                error!("Error receiving block update: {:?}", e);
                break;
            }
        }
    }

    Ok(())
}


// attempts to maintain connection to searcher service and stream pending transaction notifications over a channel
pub async fn pending_tx_loop(
    block_engine_url: String,
    auth_keypair: Arc<Keypair>,
    pending_tx_sender: Sender<PendingTxNotification>,
    backrun_pubkeys: Vec<Pubkey>,
) {
    let mut num_searcher_connection_errors: usize = 0;
    let mut num_pending_tx_sub_errors: usize = 0;
    let mut num_pending_tx_stream_errors: usize = 0;
    let mut num_pending_tx_stream_disconnects: usize = 0;

    info!("backrun pubkeys: {:?}", backrun_pubkeys);

    loop {
        sleep(Duration::from_micros(1)).await;

        match get_searcher_client(&block_engine_url, &auth_keypair).await {
            Ok(mut searcher_client) => {
                match searcher_client
                    .subscribe_mempool(MempoolSubscription {
                        regions: vec!["ny".to_string(),
                        "amsterdam".to_string(),
                        "tokyo".to_string(),
                        "frankfurt".to_string()],
                        msg: Some(mempool_subscription::Msg::ProgramV0Sub(
                            jito_protos::searcher::ProgramSubscriptionV0 { programs: backrun_pubkeys.iter().map(|pk| pk.to_string()).collect(),
                            },
                        )),
                    })
                    .await
                {
                    Ok(pending_tx_stream_response) => {
                        let mut pending_tx_stream = pending_tx_stream_response.into_inner();
                        while let Some(maybe_notification) = pending_tx_stream.next().await {
                            match maybe_notification {
                                Ok(notification) => {
                                    if pending_tx_sender.send(notification).await.is_err() {
                                        datapoint_error!(
                                            "pending_tx_send_error",
                                            ("errors", 1, i64)
                                        );
                                        return;
                                    }
                                }
                                Err(e) => {
                                    num_pending_tx_stream_errors += 1;
                                    datapoint_error!(
                                        "searcher_pending_tx_stream_error",
                                        ("errors", num_pending_tx_stream_errors, i64),
                                        ("error_str", e.to_string(), String)
                                    );
                                    break;
                                }
                            }
                        }
                        num_pending_tx_stream_disconnects += 1;
                        datapoint_error!(
                            "searcher_pending_tx_stream_disconnect",
                            ("errors", num_pending_tx_stream_disconnects, i64),
                        );
                    }
                    Err(e) => {
                        num_pending_tx_sub_errors += 1;
                        datapoint_error!(
                            "searcher_pending_tx_sub_error",
                            ("errors", num_pending_tx_sub_errors, i64),
                            ("error_str", e.to_string(), String)
                        );
                    }
                }
            }
            Err(e) => {
                num_searcher_connection_errors += 1;
                datapoint_error!(
                    "searcher_connection_error",
                    ("errors", num_searcher_connection_errors, i64),
                    ("error_str", e.to_string(), String)
                );
            }
        }
    }
}

pub async fn bundle_results_loop(endpoint: String, bundle_sender: Sender<BundleResult>) -> anyhow::Result<()> {
    // Note: Adjust the subscription request to match the data you need for bundle results.
    // This example assumes you are subscribing to some form of aggregated transaction data,
    // but you'll need to customize it based on your application's requirements.

    let mut client = GeyserGrpcClient::connect_with_timeout(
        "https://jarrett-solana-7ba9.mainnet.rpcpool.com",
        Some("8d890735-edf2-4a75-af84-92f7c9e31718"),
        None,
        Some(Duration::from_secs(10)),
        Some(Duration::from_secs(10)),
        false,
    )
    .await?;

    // Example: Using a transactions filter for this purpose, adjust as necessary.
    let transactions_filter = SubscribeRequestFilterTransactions {
        vote: None,
        failed: Some(true),
        signature: None,
        account_include: vec!["675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8".to_string()
        ,"CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK".to_string(),
        "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc".to_string()
], // Specify accounts to include in transaction updates
        account_exclude: vec![], // Specify accounts to exclude from transaction updates
        account_required: vec![], // Specify accounts required for transaction updates
    };

    let mut transactions_map: HashMap<String, SubscribeRequestFilterTransactions> = HashMap::new();
    transactions_map.insert("client".to_owned(), transactions_filter);

    let subscribe_request = SubscribeRequest {
        transactions: transactions_map,
        ..Default::default()
    };

    let (_, mut stream) = client.subscribe_with_request(Some(subscribe_request)).await?;

    while let Some(message) = stream.next().await {
        match message {
            Ok(update) => {
                // Process the update to extract or aggregate bundle results
                // This will depend on how you define a "BundleResult" in your application
                // For this example, let's assume you've processed the update into a `bundle_result`
                let bundle_result = process_update_into_bundle_result(update);

                if let Err(e) = bundle_sender.send(bundle_result).await {
                    error!("Failed to send bundle result: {:?}", e);
                    break;
                }
            }
            Err(e) => {
                error!("Error receiving data for bundle results: {:?}", e);
                break;
            }
        }
    }

    Ok(())
}

fn process_update_into_bundle_result(update: SubscribeUpdate) -> BundleResult {
    // Example: Processing the update into a `BundleResult` for this example
    // This will depend on how you define a "BundleResult" in your application
    // For this example, let's assume you've processed the update into a `bundle_result`
    let bundle_result = BundleResult {
        bundle_id: "bundle_id".to_string(),
        result: None
    };
    bundle_result

}
