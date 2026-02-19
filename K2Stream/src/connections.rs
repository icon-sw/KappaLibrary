use std::{collections::{VecDeque, vec_deque::Iter}, sync::{Arc, Mutex, mpsc::{Receiver, SyncSender}}};
use memory_macro::K2Memory;

use crate::{errors::{K2Error, K2ErrorCode}, memory::{DataHeader, MemoryTrait}};

#[derive(K2Memory)]
pub struct Input<T: 'static + Send + Sync> {
    pub name: DataHeader,
    receiver: Arc<Mutex<Receiver<T>>>,
    sender: SyncSender<T>,
}

impl<T: 'static + Send + Sync> Input<T> {
    pub fn new(name: DataHeader) -> Self {
        let (sender, receiver) = std::sync::mpsc::sync_channel(0);
        Self { name, receiver: Arc::new(Mutex::new(receiver)), sender }
    }
    pub fn get_header(&self) -> &DataHeader {
        &self.name
    }
    pub fn get_sender(&self) -> SyncSender<T> {
        self.sender.clone()
    }
    pub fn receive(&self) -> Result<T, K2Error> {
        self.receiver.lock().map_err(|_| K2Error { code: K2ErrorCode::LockError, message: "Failed to lock receiver".into() })?.recv().map_err(|_| K2Error { code: K2ErrorCode::NotFound, message: "Failed to receive data".into() })
    }
}
#[derive(K2Memory)]
pub struct Output<T: 'static + Send + Sync> {
    pub name: DataHeader,
    sender: Vec<SyncSender<T>>,
}

impl<T: 'static + Send + Sync + Clone> Output<T> {
    pub fn new(name: DataHeader) -> Self {
        Self { name, sender: Vec::new() }
    }
    pub fn connect(&mut self, sender: SyncSender<T>) {
        self.sender.push(sender);
    }
    pub fn get_header(&self) -> &DataHeader {
        &self.name
    }
    pub fn send(&self, data: T) -> Result<(), K2Error> {
        for sender in &self.sender {
            sender.send(data.clone()).map_err(|_| K2Error { code: K2ErrorCode::NotFound, message: "Failed to send data".into() })?;
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
    pub fn get_nodes_iter(&self) -> Iter<'_, String> {
        self.nodes.iter()
    }
    pub fn get_nodes(&self) -> Vec<String> {
        self.nodes.iter().cloned().collect()
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
                let from_index = self.get_nodes_iter().position(|item| item == &from.clone());
                let to_index = self.get_nodes_iter().position(|item| item == &to.clone());
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
    pub fn check(&mut self) -> Result<(),K2Error> {
        if !self.is_sorted() {
            self.sort();
        }
        for connection in self.connections.clone() {
            let from = connection.from();
            let to = connection.to();
            let from_index = self.get_nodes_iter().position(|item| item == &from.clone());
            let to_index = self.get_nodes_iter().position(|item| item == &to.clone());
            if from_index > to_index {
                return Err(K2Error { code: K2ErrorCode::BadFormat, message: format!("Invalid connection from {} to {}", from, to) });
            }
        }
        Ok(())
    }
}