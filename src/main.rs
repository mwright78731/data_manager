use std::collections::HashMap;
use std::path::PathBuf;
use data_manager::{DataManager, DataChunk, HttpDataManager, DataChunkRef};

#[tokio::main]
async fn main() {
    let data_dir = PathBuf::from("data");
    let manager = HttpDataManager::new(data_dir);

    let mut files: HashMap<String, String> = HashMap::new();
    files.insert("file1.txt".to_string(), "https://file-examples.com/storage/fe524cbe5b66e6d25aa9bb6/2017/10/file-example_PDF_1MB.pdf".to_string());
    files.insert("file2.txt".to_string(), "https://file-examples.com/storage/fe524cbe5b66e6d25aa9bb6/2017/10/file-example_PDF_1MB.pdf".to_string());
    files.insert("file3.txt".to_string(), "https://file-examples.com/storage/fe524cbe5b66e6d25aa9bb6/2017/10/file-example_PDF_1MB.pdf".to_string());
    files.insert("file4.txt".to_string(), "https://file-examples.com/storage/fe524cbe5b66e6d25aa9bb6/2017/10/file-example_PDF_1MB.pdf".to_string());
    files.insert("file5.txt".to_string(), "https://file-examples.com/storage/fe524cbe5b66e6d25aa9bb6/2017/10/file-example_PDF_1MB.pdf".to_string());
    files.insert("file6.txt".to_string(), "https://file-examples.com/storage/fe524cbe5b66e6d25aa9bb6/2017/10/file-example_PDF_1MB.pdf".to_string());

    let chunk = DataChunk::new([0; 32], [0; 32], 0..1000, files); 

    println!("Downloading chunk");
    manager.download_chunk(chunk);

    // Wait for the download to finish. 
    // If not all files are downloaded, rerunning the program will resume the download.
    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

    let chunks = manager.list_chunks();
    println!("Chunks: {:?}", chunks);

    let block_number = 0;
    match manager.find_chunk([0; 32], block_number) {
        Some(chunk_ref) => {
            println!("Chunk path: {:?}", chunk_ref.path());
            manager.delete_chunk([0; 32]);
            println!("Chunk deleted.");
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }
        None => {
            println!("Chunk not found for block number {}", block_number);
        }
    }

    let chunks = manager.list_chunks();
    println!("Chunks: {:?}", chunks);
}
