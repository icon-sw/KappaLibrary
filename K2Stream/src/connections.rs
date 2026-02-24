use std::{collections::{VecDeque, vec_deque::Iter}, sync::{Arc, Mutex, mpsc::{Receiver, SyncSender}}};
use memory_macro::K2Memory;

use crate::{errors::{K2Error, K2ErrorCode}, k2err, memory::{DataHeader, MemoryTrait}};

#[derive(K2Memory)]
pub struct Input<T: 'static + Send + Sync> {
    pub name: DataHeader,
    receiver: Arc<Mutex<Receiver<T>>>,
    sender: SyncSender<T>,
}

impl<T: 'static + Send + Sync> Input<T> {
    pub fn new(name: DataHeader) -> Self {
        let (sender, receiver) = std::sync::mpsc::sync_channel(100);
        Self { name, receiver: Arc::new(Mutex::new(receiver)), sender }
    }
    pub fn get_header(&self) -> &DataHeader {
        &self.name
    }
    pub fn get_sender(&self) -> SyncSender<T> {
        self.sender.clone()
    }
    pub fn receive(&self) -> Result<T, K2Error> {
        self.receiver
            .lock().map_err(|_| k2err!( K2ErrorCode::LockError, "Failed to lock receiver"))?
            .recv().map_err(|_| k2err!( K2ErrorCode::NotFound, "Failed to receive data"))
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
            sender.send(data.clone()).map_err(|_| k2err!( K2ErrorCode::NotFound, "Failed to send data"))?;
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
#[derive(Clone)]
pub struct ConnectionGraph {
    nodes: VecDeque<String>,
    connections: Vec<Connection>,
    sorted: bool,
}

impl Default for ConnectionGraph {
    fn default() -> Self {
        Self::new()
    }
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
    pub fn add_connection(&mut self, from: String, to: String) {
        self.connections.push(Connection { from, to });
        self.sorted = false;
    }
    fn sort(&mut self) {
        if !self.sorted {
            self.nodes.clear();
            let mut sorted_nodes: VecDeque<String> =VecDeque::new();
            for connection in self.connections.clone() {
                let from = connection.from();
                let to = connection.to();
                dbg!(format!("Processing {}->{}", from.clone(), to.clone()));
                let from_index = sorted_nodes.iter().position(|item| item == &from.clone());
                let to_index = sorted_nodes.iter().position(|item| item == &to.clone());
                if let Some(to_index) = to_index {
                    if let Some(from_index) = from_index {
                        if from_index > to_index {
                            dbg!(format!("Removing {} from original position", from.clone()));
                            sorted_nodes.remove(from_index);
                            dbg!(format!("Adding {} after {}", from.clone(), to.clone()));
                            sorted_nodes.insert(to_index, from.clone());
                        }
                    } else {
                        dbg!(format!("Adding {} after {}", from.clone(), to.clone()));
                        sorted_nodes.insert(to_index, from.clone());
                    }
                } else {
                    if from_index.is_none() {
                        dbg!(format!("Adding {}", from.clone()));
                        sorted_nodes.push_back(from.clone());
                    }
                    dbg!(format!("Adding {}", to.clone()));
                    sorted_nodes.push_back(to.clone());
                }
                dbg!(format!("{:?}", sorted_nodes));
            }
            self.nodes = sorted_nodes;
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
                return Err(k2err!( K2ErrorCode::BadFormat, format!("Invalid connection from {} to {}", from, to)));
            }
        }
        self.sorted = true;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_io() {
        let input_test = Input::<String>::new("test_input".to_string());
        assert_eq!(input_test.get_header(), &"test_input".to_string());
        let mut output_test =  Output::<String>::new("test_output".to_string());
        assert_eq!(output_test.get_header(), &"test_output".to_string());
        let sender = input_test.get_sender();
        output_test.connect(sender);
        assert!(output_test.send("hello".to_string()).is_ok());
        let input = input_test.receive();
        assert!(input.is_ok());
        if let Ok(res) = input {
            assert_eq!(res, "hello".to_string());
        }
    }
    #[test]
    fn test_connection_graph() {
        let mut graph = ConnectionGraph::new();
        graph.add_connection("test_1".to_string(), "test_2".to_string());
        graph.add_connection("test_3".to_string(), "test_4".to_string());
        graph.add_connection("test_3".to_string(), "test_2".to_string());
        graph.add_connection("test_5".to_string(), "test_4".to_string());

        assert!(!graph.is_sorted());
        assert!(graph.check().is_ok());
        assert!(graph.is_sorted());
        assert_eq!(graph.nodes[0], "test_1".to_string());
        assert_eq!(graph.nodes[1], "test_3".to_string());
        assert_eq!(graph.nodes[2], "test_2".to_string());
        assert_eq!(graph.nodes[3], "test_5".to_string());
        assert_eq!(graph.nodes[4], "test_4".to_string());
        graph.add_connection("test_2".to_string(), "test_1".to_string());
        assert!(!graph.is_sorted());
        assert!(graph.check().is_err());

    }
}