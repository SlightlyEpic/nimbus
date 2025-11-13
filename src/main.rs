use nimbus::storage;

fn main() {
    let dm = storage::disk::DiskManager::new("/var/lib/nimbus".into());
}
