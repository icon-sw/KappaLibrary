use std::process::Command;

pub struct CargoInterface {
    pub cargo_path: String,
    pub library_path: String,
}

impl CargoInterface {
    pub fn cargo_add_commands(&self, path: String) -> Result<(), String> {
        let curr_dir = std::env::current_dir().unwrap();
        println!("Setting current dir to {}", path);
        if std::env::set_current_dir(&path).is_err() {
            return Err(format!("Something went wrong in crate creation"));
        }
        println!("Library path: {}", self.library_path);
        Command::new(&self.cargo_path).arg("add").arg("k2stream").arg("--path").arg(format!("{}/k2stream", self.library_path)).status().expect("Failed to create the project");
        Command::new(&self.cargo_path).arg("add").arg("k2lang").arg("--path").arg(format!("{}/k2lang", self.library_path)).status().expect("Failed to create the project");
        Command::new(&self.cargo_path).arg("add").arg("processor_macro").arg("--path").arg(format!("{}/macro/processor_macro", self.library_path)).status().expect("Failed to create the project");
        Command::new(&self.cargo_path).arg("add").arg("memory_macro").arg("--path").arg(format!("{}/macro/memory_macro", self.library_path)).status().expect("Failed to create the project");
        let res = std::env::set_current_dir(curr_dir);
        match res {
            Ok(_) => (),
            Err(e) => return Err(format!("Failed to set back the current directory: {}", e)),
        }
        Ok(())
    }

    pub fn cargo_new_library(&self, path: String) -> Result<(), String> {
        Command::new(&self.cargo_path).arg("new").arg("--lib").arg(&path).status().expect("Failed to create the project");
        Ok(())
    }
    pub fn cargo_new_application(&self, path: String) -> Result<(), String> {
        Command::new(&self.cargo_path).arg("new").arg(&path).status().expect("Failed to create the project");
        Ok(())
    }

    pub fn cargo_build(&self, path: String, build_type: String) -> Result<(), String> {
        let curr_dir = std::env::current_dir().unwrap();
        println!("Setting current dir to {}", path);
        if std::env::set_current_dir(&path).is_err() {
            return Err(format!("Something went wrong in crate creation"));
        }
        if build_type == "debug" {
            Command::new(&self.cargo_path).arg("build").status().expect("Failed to build the project");
        } else {
            Command::new(&self.cargo_path).arg("build").arg(format!("--{}", build_type)).status().expect("Failed to build the project");
        }
        let res = std::env::set_current_dir(curr_dir);
        match res {
            Ok(_) => (),
            Err(e) => return Err(format!("Failed to set back the current directory: {}", e)),
        }
        Ok(())
    }
    pub fn delete_project(&self, path: String) -> Result<(), String> {
        match std::fs::remove_dir_all(path.clone()) {
            Ok(_) => Ok(()),
            Err(e) => Err(format!("Error deleting project at {}: {}", path, e)),
        }
    }
}