use std::collections::HashMap;

use crate::modes::OperativeMode;

pub struct StreamController {
    modes: HashMap<usize, OperativeMode>,
    current_mode_id: usize,
}

impl StreamController {
    pub fn new() -> Self {
        let mode = OperativeMode::new("default".to_string(), 0);
        let mut modes = HashMap::new();
        modes.insert(0, mode);
        Self {
            modes,
            current_mode_id: 0,
        }
    }
    pub fn add_mode(&mut self, id: usize, mode: OperativeMode) -> Result<(), ()> {
        if self.modes.contains_key(&id) {
            Err(())
        } else {
            self.modes.insert(id, mode);
            Ok(())
        }
    }
    pub fn set_current_mode(&mut self, id: usize) -> Result<(), ()> {
        if self.modes.contains_key(&id) {
            let mode = self.modes.get_mut(&self.current_mode_id).ok_or(())?;
            mode.finalize()?;
            let mode = self.modes.get_mut(&id).ok_or(())?;
            mode.initialize()?;
            mode.process()?;
            self.current_mode_id = id;
            Ok(())
        } else {
            Err(())
        }
    }
}