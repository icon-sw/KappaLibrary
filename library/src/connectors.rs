use std::sync::mpsc::{Receiver, SyncSender};
use crate::memory::{MemoryTrait, DataHeader};

pub struct Input<T> {
    header: DataHeader,
    receiver: Receiver<T>,
    sender: SyncSender<T>,
}

impl<T> Input<T> {
    pub fn new(header: DataHeader) -> Self {
        let (sender, receiver) = std::sync::mpsc::sync_channel(0);
        Self { header, receiver, sender }
    }
    pub fn get_header(&self) -> &DataHeader {
        &self.header
    }
    pub fn get_sender(&self) -> SyncSender<T> {
        self.sender.clone()
    }
    pub fn receive(&self) -> Result<T, ()> {
        self.receiver.recv().map_err(|_| ())
    }
}

impl MemoryTrait for Input<u8> {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub struct Output<T> {
    header: DataHeader,
    sender: Vec<SyncSender<T>>,
}

impl<T: Clone> Output<T> {
    pub fn new(header: DataHeader) -> Self {
        Self { header, sender: Vec::new() }
    }
    pub fn connect(&mut self, sender: SyncSender<T>) {
        self.sender.push(sender);
    }
    pub fn get_header(&self) -> &DataHeader {
        &self.header
    }
    pub fn send(&self, data: T) -> Result<(), ()> {
        for sender in &self.sender {
            sender.send(data.clone()).map_err(|_| ())?;
        }
        Ok(())
    }
}