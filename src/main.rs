mod catalog;
mod constants;
mod execution;
mod rt_type;
mod storage;
fn main() {
    let dm = storage::disk::DiskManager::new("/var/lib/nimbus".into());
}
