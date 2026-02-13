pub enum ProcessorCoderParts {
    HeadMod,
    UsedDefinedCode,
    HeadStruct,
    UserDefinedStruct,
    EndStruct,
    HeadBuilder,
    UserDefinedBuilder,
    UserMemberCreation,
    UserDefinedImplStruct,
    InitBody,
    RunBody,
    ProcessBody,
    StopBody,
}

impl TryFrom<u8> for ProcessorCoderParts {
    type Error = ();
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(ProcessorCoderParts::HeadMod),
            1 => Ok(ProcessorCoderParts::UsedDefinedCode),
            2 => Ok(ProcessorCoderParts::HeadStruct),
            3 => Ok(ProcessorCoderParts::UserDefinedStruct),
            4 => Ok(ProcessorCoderParts::EndStruct),
            5 => Ok(ProcessorCoderParts::HeadBuilder),
            6 => Ok(ProcessorCoderParts::UserDefinedBuilder),
            7 => Ok(ProcessorCoderParts::UserMemberCreation),
            8 => Ok(ProcessorCoderParts::UserDefinedImplStruct),
            9 => Ok(ProcessorCoderParts::InitBody),
            10 => Ok(ProcessorCoderParts::RunBody),
            11 => Ok(ProcessorCoderParts::ProcessBody),
            12 => Ok(ProcessorCoderParts::StopBody),
            _ => Err(()),
        }
    }
}
impl TryFrom<String> for ProcessorCoderParts {
    type Error = ();
    fn try_from(value: String) -> Result<Self, Self::Error> {
        let num_result: Result<u8, _> = value.parse();
        let int_code: u8 = match num_result {
            Ok(n) => n,
            Err(_) => {return Err(());},
        };
        Self::try_from(int_code)
    }
}