package processing

/*
SysmonCoreECS is a concise decoder struct for fetching only the fields we are interested in
Sub structs are immutable in order to omit constant nil pointer checks when fetching fields
Omitting pointers also makes the struct safe to copy between workers
*/
type SysmonCoreECS struct {
	Process struct {
		Name        string `json:"name"`
		CommandLine string `json:"command_line,omitempty"`
		Executable  string `json:"executable"`
		EntityID    string `json:"entity_id"`
		Hash        struct {
			Md5    string `json:"md5"`
			Sha1   string `json:"sha1"`
			Sha256 string `json:"sha256"`
		} `json:"hash"`
		Parent struct {
			CommandLine string `json:"command_line"`
			Executable  string `json:"executable"`
			Name        string `json:"name"`
			Pid         int    `json:"pid"`
			EntityID    string `json:"entity_id"`
		} `json:"parent"`
		Pe struct {
			Company          string `json:"company"`
			Description      string `json:"description"`
			FileVersion      string `json:"file_version"`
			Imphash          string `json:"imphash"`
			OriginalFileName string `json:"original_file_name"`
			Product          string `json:"product"`
		} `json:"pe"`
		Pid              int    `json:"pid"`
		WorkingDirectory string `json:"working_directory,omitempty"`
	} `json:"process"`
	User struct {
		Domain string `json:"domain"`
		ID     string `json:"id"`
		Name   string `json:"name"`
	} `json:"user"`
	Network struct {
		CommunityID string `json:"community_id,omitempty"`
		Direction   string `json:"direction,omitempty"`
		Transport   string `json:"transport,omitempty"`
	} `json:"network"`
	Winlog struct {
		API          string `json:"api"`
		Channel      string `json:"channel"`
		ComputerName string `json:"computer_name"`
		EventID      string `json:"event_id"`
		Opcode       string `json:"opcode"`
	} `json:"winlog"`
}
