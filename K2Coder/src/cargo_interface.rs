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
        Command::new(&self.cargo_path).arg("add").arg("num-traits").status().expect("Failed to create the project");
        Command::new(&self.cargo_path).arg("add").arg("serde_json").status().expect("Failed to create the project");
        Command::new(&self.cargo_path).arg("add").arg("serde").arg("--features").arg("derive").status().expect("Failed to create the project");
        Command::new(&self.cargo_path).arg("add").arg("processor_engine").arg("--path").arg(format!("{}/processor_engine", self.library_path)).status().expect("Failed to create the project");
        Command::new(&self.cargo_path).arg("add").arg("stream_proc_macro").arg("--path").arg(format!("{}/processor_engine/src/stream_proc_macro", self.library_path)).status().expect("Failed to create the project");
        Command::new(&self.cargo_path).arg("add").arg("data_model").arg("--path").arg(format!("{}/data_model", self.library_path)).status().expect("Failed to create the project");
        Command::new(&self.cargo_path).arg("add").arg("utils").arg("--path").arg(format!("{}/utils", self.library_path)).status().expect("Failed to create the project");
        Command::new("mkdir").arg("-p").arg(format!("{}/.project", path)).status().expect("Failed to create the .project folder");
        let res = std::env::set_current_dir(curr_dir);
        match res {
            Ok(_) => (),
            Err(e) => return Err(format!("Failed to set back the current directory: {}", e)),
        }
        Ok(())
    }

    pub fn cargo_new_library(&self, path: String) -> Result<String, String> {
        let output = Command::new(&self.cargo_path).arg("new").arg("--lib").arg(&path).output().expect("Failed to create the project");
        if output.status.success() {
            Ok(String::from_utf8_lossy(&output.stdout).to_string())
        } else {
            Err(String::from_utf8_lossy(&output.stderr).to_string())
        }
    }
    pub fn cargo_new_application(&self, path: String) -> Result<String, String> {
        let output = Command::new(&self.cargo_path).arg("new").arg(&path).output().expect("Failed to create the project");
        if output.status.success() {
            Ok(String::from_utf8_lossy(&output.stdout).to_string())
        } else {
            Err(String::from_utf8_lossy(&output.stderr).to_string())
        }
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