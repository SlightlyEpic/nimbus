pub trait SerdeFixed<const SIZE: usize> {
    fn serialize(self: &Self) -> [u8; SIZE];
    fn deserialize(data: &[u8; SIZE]) -> Self;
}

pub trait SerdeDyn {
    fn serialize(self: &Self) -> (&[u8], usize);
    fn deserialize(raw: &[u8]) -> Self;
}
