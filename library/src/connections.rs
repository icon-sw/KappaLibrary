use std::{collections::{VecDeque, vec_deque::Iter}, sync::mpsc::{Receiver, SyncSender}};
use crate::memory::{MemoryTrait, DataHeader};

pub struct Input<T> {
    pub name: DataHeader,
    receiver: Receiver<T>,
    sender: SyncSender<T>,
}

impl<T> Input<T> {
    pub fn new(name: DataHeader) -> Self {
        let (sender, receiver) = std::sync::mpsc::sync_channel(0);
        Self { name, receiver, sender }
    }
    pub fn get_header(&self) -> &DataHeader {
        &self.name
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
    pub name: DataHeader,
    sender: Vec<SyncSender<T>>,
}

impl<T: Clone> Output<T> {
    pub fn new(name: DataHeader) -> Self {
        Self { name, sender: Vec::new() }
    }
    pub fn connect(&mut self, sender: SyncSender<T>) {
        self.sender.push(sender);
    }
    pub fn get_header(&self) -> &DataHeader {
        &self.name
    }
    pub fn send(&self, data: T) -> Result<(), ()> {
        for sender in &self.sender {
            sender.send(data.clone()).map_err(|_| ())?;
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct Connection {
    from: String,
    to: String,
}

impl Connection {
    pub fn from(&self) -> &String { &self.from }
    pub fn to(&self) -> &String { &self.to }
}

pub struct ConnectionGraph {
    nodes: VecDeque<String>,
    connections: Vec<Connection>,
    sorted: bool,
}

impl ConnectionGraph {
    pub fn new() -> Self {
        Self {
            nodes: VecDeque::new(),
            connections: Vec::new(),
            sorted: false,
        }
    }
    pub fn is_sorted(&self) -> bool {
        self.sorted
    }
    pub fn get_nodes(&self) -> Iter<'_, String> {
        self.nodes.iter()
    }
    pub fn add_connection(&mut self, from: String, to: String) {
        self.connections.push(Connection { from, to });
        self.sorted = false;
    }
    fn sort(&mut self) {
        if !self.sorted {
            self.nodes.clear();
            for connection in self.connections.clone() {
                let from = connection.from();
                let to = connection.to();
                let from_index = self.get_nodes().position(|item| item == &from.clone());
                let to_index = self.get_nodes().position(|item| item == &to.clone());
                if let Some(to_index) = to_index {
                    if let Some(from_index) = from_index {
                        if from_index > to_index {
                            self.nodes.remove(from_index);
                            self.nodes.insert(to_index, from.clone());
                        }
                    } else {
                        self.nodes.insert(to_index, from.clone());
                    }
                } else {
                    if from_index.is_none() {
                        self.nodes.push_back(from.clone());
                    }
                    self.nodes.push_back(to.clone());
                }
            }
        }
    }
    pub fn check(&mut self) -> Result<(),()> {
        if !self.is_sorted() {
            self.sort();
        }
        for connection in self.connections.clone() {
            let from = connection.from();
            let to = connection.to();
            let from_index = self.get_nodes().position(|item| item == &from.clone());
            let to_index = self.get_nodes().position(|item| item == &to.clone());
            if from_index > to_index {
                return Err(());
            }
        }
        Ok(())
    }
}