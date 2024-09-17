use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::ops::Range;
use tokio::fs::{create_dir_all, remove_dir_all, File};
use tokio::io::AsyncWriteExt;
use reqwest::Client;
use tokio::sync::Semaphore;
use tokio::fs;

pub type DatasetId = [u8; 32];
pub type ChunkId = [u8; 32];

const MAX_DOWNLOAD_PARALLELISM: usize = 30; // Total number of parallel downloads 
const MAX_STORAGE_SIZE: u64 = 1 * 1024 * 1024 * 1024 * 1024; // 1 TB in bytes
const MIN_FILE_SPACE: u64 = 10 * 1024 * 1024;


/// data chunk description
pub struct DataChunk {
    id: ChunkId,
    /// Dataset (blockchain) id
    dataset_id: DatasetId,
    /// Block range this chunk is responsible for (around 100 - 10000 blocks)
    block_range: Range<u64>,
    /// Data chunk files. 
    /// A mapping between file names and HTTP URLs to download files from.
    /// Usually contains 1 - 10 files of various sizes. 
    /// The total size of all files in the chunk is about 200 MB.
    files: HashMap<String, String>
}

impl DataChunk {
    pub fn new(
        id: ChunkId,
        dataset_id: DatasetId,
        block_range: Range<u64>,
        files: HashMap<String, String>,
    ) -> Self {
        DataChunk {
            id,
            dataset_id,
            block_range,
            files,
        }
    }
}

pub trait DataManager: Send + Sync {
    /// Create a new `DataManager` instance, that will use `data_dir` to store the data.
    /// `DataManager` must pick up the state left from the previous run when `data_dir` is not empty.
    /// It is ok to loose partiallay downloaded chunks.
    fn new(data_dir: PathBuf) -> Self;
    /// Schedule `chunk` download in background
    fn download_chunk(&self, chunk: DataChunk);
    // List chunks, that are currently available
    fn list_chunks(&self) -> Vec<ChunkId>;
    /// Find a chunk from a given dataset, that is responsible for `block_number`.
    fn find_chunk(&self, dataset_id: [u8; 32], block_number: u64) -> Option<impl DataChunkRef>;
    /// Schedule data chunk for deletion in background
    fn delete_chunk(&self, chunk_id: [u8; 32]);
}


// Data chunk must remain available and untouched till this reference is not dropped
pub trait DataChunkRef: Send + Sync + Clone {
    // Data chunk directory
    fn path(&self) -> &Path;
}

#[derive(Clone)]
struct HttpDataChunkRef {
    path: PathBuf,
}

impl DataChunkRef for HttpDataChunkRef {
    fn path(&self) -> &Path {
        &self.path
    }
}

pub struct HttpDataManager {
    data_dir: PathBuf,
    chunks: Arc<Mutex<HashMap<ChunkId, DataChunk>>>,
    current_data_size: Arc<Mutex<u64>>,
    thread_limit_semaphore: Arc<Semaphore>,
}

impl HttpDataManager {
    async fn calculate_directory_size(path: &Path) -> u64 {
        let mut total_size = 0;
        let mut stack = vec![path.to_path_buf()];

        while let Some(current_path) = stack.pop() {
            let mut dir = fs::read_dir(&current_path).await.expect("Failed to read directory");

            while let Some(entry) = dir.next_entry().await.expect("Failed to read directory entry") {
                let metadata = entry.metadata().await.expect("Failed to read metadata");
                if metadata.is_dir() {
                    stack.push(entry.path());
                } else {
                    total_size += metadata.len();
                }
            }
        }

        total_size
    }

    async fn async_new(data_dir: PathBuf) -> Self {
        let initial_size = Self::calculate_directory_size(&data_dir).await;
        HttpDataManager {
            data_dir,
            chunks: Arc::new(Mutex::new(HashMap::new())),
            current_data_size: Arc::new(Mutex::new(initial_size)),
            thread_limit_semaphore: Arc::new(Semaphore::new(MAX_DOWNLOAD_PARALLELISM)),
        }
    }
}

impl DataManager for HttpDataManager {
    fn new(data_dir: PathBuf) -> Self {
        tokio::task::block_in_place(|| {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                HttpDataManager::async_new(data_dir).await
            })
        })
    }

    fn download_chunk(&self, chunk: DataChunk) {
        let data_dir = self.data_dir.clone();
        let chunks = self.chunks.clone();
        let current_size = self.current_data_size.clone();
        let thread_limit_semaphore = self.thread_limit_semaphore.clone();
        let failed_download_count = Arc::new(Mutex::new(0));
        
        tokio::spawn(async move {
            let chunk_dir = data_dir.join(hex::encode(chunk.id));
            create_dir_all(&chunk_dir).await.expect("Failed to create chunk directory");
    
            let client = Client::new();
            let mut handles = vec![];
    
            for (file_name, url) in chunk.files.clone() {
                let file_path = chunk_dir.join(&file_name);
                let complete_file_path = chunk_dir.join(format!("{}.complete", file_name));
                let error_file_path = chunk_dir.join(format!("{}.error", file_name));
                let client = client.clone();
                let permit = thread_limit_semaphore.clone().acquire_owned().await.unwrap();
                let failed_download_count = failed_download_count.clone();
                let current_size = current_size.clone();
    
                let handle = tokio::spawn(async move {
                    let _permit = permit; // Keep the permit in scope
    
                    // Check if both the target file and the .complete file exist
                    if file_path.exists() && complete_file_path.exists() {
                        println!("File {} already downloaded, skipping.", file_name);
                        return;
                    }
    
                    // Delete any existing .error file
                    if error_file_path.exists() {
                        tokio::fs::remove_file(&error_file_path).await.expect("Failed to delete .error file");
                    }
    
                    // Check available space before downloading the file
                    // Instead of getting the file size from the server, we assume a fixed size
                    // and then true up after the download is complete.
                    // Therefore, we could slightly exceed the MAX_STORAGE_SIZE limit.
                    {
                        let mut current_size_guard = current_size.lock().unwrap();
                        if *current_size_guard + MIN_FILE_SPACE > MAX_STORAGE_SIZE {
                            eprintln!("Error: Not enough space to download file {}", file_name);
                            let mut failed_count_guard = failed_download_count.lock().unwrap();
                            *failed_count_guard += 1;
                            return;
                        }
                    
                        // Temporarily increase the current size by the assumed file size
                        *current_size_guard += MIN_FILE_SPACE;
                    } // Release the lock by ending the scope
                    
                    // Download the file
                    let mut file = File::create(&file_path).await.expect("Failed to create file");
                    let response = client.get(url).send().await;
    
                    match response {
                        Ok(mut response) => {
                            if response.status().is_success() {
                                if response.headers().get("content-type").map_or(false, |v| v == "text/html") {
                                    // Write the HTML error page to the .error file
                                    let mut error_file = File::create(&error_file_path).await.expect("Failed to create .error file");
                                    while let Some(chunk) = response.chunk().await.expect("Failed to read chunk") {
                                        error_file.write_all(&chunk).await.expect("Failed to write to .error file");
                                    }
                                    let mut failed_count_guard = failed_download_count.lock().unwrap();
                                    *failed_count_guard += 1;
                                } else {
                                    // Write the response to the file
                                    let mut actual_file_size = 0;
                                    while let Some(chunk) = response.chunk().await.expect("Failed to read chunk") {
                                        actual_file_size += chunk.len() as u64;
                                        file.write_all(&chunk).await.expect("Failed to write to file");
                                    }
                                    // Create the .complete file
                                    File::create(&complete_file_path).await.expect("Failed to create .complete file");
    
                                    // Update current size with the actual file size
                                    let mut current_size_guard = current_size.lock().unwrap();
                                    *current_size_guard += actual_file_size;
                                    *current_size_guard -= MIN_FILE_SPACE;
                                }
                            } else {
                                // Write the error status to the .error file
                                let mut error_file = File::create(&error_file_path).await.expect("Failed to create .error file");
                                let error_message = format!("HTTP error: {}", response.status());
                                error_file.write_all(error_message.as_bytes()).await.expect("Failed to write to .error file");
                                let mut failed_count_guard = failed_download_count.lock().unwrap();
                                *failed_count_guard += 1;
                            }
                        }
                        Err(e) => {
                            // Write the error to the .error file
                            let mut error_file = File::create(&error_file_path).await.expect("Failed to create .error file");
                            error_file.write_all(e.to_string().as_bytes()).await.expect("Failed to write to .error file");
                            let mut failed_count_guard = failed_download_count.lock().unwrap();
                            *failed_count_guard += 1;
                        }
                    }
                });
                handles.push(handle);
            }
    
            for handle in handles {
                handle.await.expect("File download task failed");
            }
    
            let failed_download_count = *failed_download_count.lock().unwrap();
            if failed_download_count == 0 {
                chunks.lock().unwrap().insert(chunk.id, chunk);
            } else {
                // Rollback the assumed sizes if not all files were downloaded successfully
                let mut current_size_guard = current_size.lock().unwrap();
                *current_size_guard -= failed_download_count as u64 * MIN_FILE_SPACE;
            }
        });
    }

    fn list_chunks(&self) -> Vec<ChunkId> {
        let chunks = self.chunks.lock().unwrap();
        chunks.keys().cloned().collect()
    }

    fn find_chunk(&self, dataset_id: [u8; 32], block_number: u64) -> Option<impl DataChunkRef> {
        let chunks = self.chunks.lock().unwrap();
        for chunk in chunks.values() {
            if chunk.dataset_id == dataset_id && chunk.block_range.contains(&block_number) {
                return Some(HttpDataChunkRef {
                    path: self.data_dir.join(hex::encode(chunk.id)),
                });
            }
        }
        None
    }

    fn delete_chunk(&self, chunk_id: [u8; 32]) {
        let data_dir = self.data_dir.clone();
        let chunks = self.chunks.clone();
        let current_size = self.current_data_size.clone();

        tokio::spawn(async move {
            let chunk_dir = data_dir.join(hex::encode(chunk_id));

            // probably better to store the size in the chunk itself
            let chunk_size = HttpDataManager::calculate_directory_size(&chunk_dir).await;
            if let Err(e) = remove_dir_all(&chunk_dir).await {
                eprintln!("Failed to delete chunk directory: {}", e);
            }

            let mut chunks = chunks.lock().unwrap();
            chunks.remove(&chunk_id);

            let mut current_size_guard = current_size.lock().unwrap();
            *current_size_guard += chunk_size;
        });
    }
}