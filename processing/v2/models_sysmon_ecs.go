package processing

import (
	"time"
)

/*
SysmonECS Full fat sysmon event in ECS format generated from sample data
Mostly for reference, as we don't need everything
*/
type SysmonECS struct {
	Agent struct {
		EphemeralID string `json:"ephemeral_id"`
		Hostname    string `json:"hostname"`
		ID          string `json:"id"`
		Name        string `json:"name"`
		Type        string `json:"type"`
		Version     string `json:"version"`
	} `json:"agent"`
	Destination *struct {
		Domain string `json:"domain"`
		Ip     string `json:"ip"`
		Port   int    `json:"port"`
	} `json:"destination,omitempty"`
	Dns *struct {
		Answers []struct {
			Data string `json:"data"`
			Type string `json:"type"`
		} `json:"answers"`
		Question struct {
			Name string `json:"name"`
		} `json:"question"`
		ResolvedIp []string `json:"resolved_ip"`
	} `json:"dns,omitempty"`
	Ecs struct {
		Version string `json:"version"`
	} `json:"ecs"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error,omitempty"`
	Event struct {
		Action   string    `json:"action"`
		Category []string  `json:"category,omitempty"`
		Code     string    `json:"code"`
		Created  time.Time `json:"created"`
		Kind     string    `json:"kind"`
		Module   string    `json:"module,omitempty"`
		Provider string    `json:"provider"`
		Type     []string  `json:"type,omitempty"`
	} `json:"event"`
	File *struct {
		CodeSignature *struct {
			Signed      bool   `json:"signed"`
			Status      string `json:"status"`
			SubjectName string `json:"subject_name"`
			Valid       bool   `json:"valid"`
		} `json:"code_signature,omitempty"`
		Directory string `json:"directory,omitempty"`
		Extension string `json:"extension,omitempty"`
		Hash      *struct {
			Md5    string `json:"md5"`
			Sha1   string `json:"sha1"`
			Sha256 string `json:"sha256"`
		} `json:"hash,omitempty"`
		Name string `json:"name"`
		Path string `json:"path,omitempty"`
		Pe   *struct {
			Company          string `json:"company"`
			Description      string `json:"description"`
			FileVersion      string `json:"file_version"`
			Imphash          string `json:"imphash,omitempty"`
			OriginalFileName string `json:"original_file_name"`
			Product          string `json:"product"`
		} `json:"pe,omitempty"`
	} `json:"file,omitempty"`
	Hash *struct {
		Imphash string `json:"imphash,omitempty"`
		Md5     string `json:"md5"`
		Sha1    string `json:"sha1"`
		Sha256  string `json:"sha256"`
	} `json:"hash,omitempty"`
	Host struct {
		Architecture string   `json:"architecture"`
		Hostname     string   `json:"hostname"`
		ID           string   `json:"id"`
		Ip           []string `json:"ip"`
		Mac          []string `json:"mac"`
		Name         string   `json:"name"`
		OS           struct {
			Build    string `json:"build"`
			Family   string `json:"family"`
			Kernel   string `json:"kernel"`
			Name     string `json:"name"`
			Platform string `json:"platform"`
			Type     string `json:"type"`
			Version  string `json:"version"`
		} `json:"os"`
	} `json:"host"`
	Log struct {
		Level string `json:"level"`
	} `json:"log"`
	Message string `json:"message"`
	Network *struct {
		CommunityID string `json:"community_id,omitempty"`
		Direction   string `json:"direction,omitempty"`
		Protocol    string `json:"protocol"`
		Transport   string `json:"transport,omitempty"`
		Type        string `json:"type,omitempty"`
	} `json:"network,omitempty"`
	Process *struct {
		Args        []string `json:"args,omitempty"`
		CommandLine string   `json:"command_line,omitempty"`
		EntityID    string   `json:"entity_id"`
		Executable  string   `json:"executable"`
		Hash        *struct {
			Md5    string `json:"md5"`
			Sha1   string `json:"sha1"`
			Sha256 string `json:"sha256"`
		} `json:"hash,omitempty"`
		Name   string `json:"name"`
		Parent *struct {
			Args        []string `json:"args"`
			CommandLine string   `json:"command_line"`
			EntityID    string   `json:"entity_id"`
			Executable  string   `json:"executable"`
			Name        string   `json:"name"`
			Pid         int      `json:"pid"`
		} `json:"parent,omitempty"`
		Pe *struct {
			Company          string `json:"company"`
			Description      string `json:"description"`
			FileVersion      string `json:"file_version"`
			Imphash          string `json:"imphash"`
			OriginalFileName string `json:"original_file_name"`
			Product          string `json:"product"`
		} `json:"pe,omitempty"`
		Pid    int `json:"pid"`
		Thread *struct {
			ID int `json:"id"`
		} `json:"thread,omitempty"`
		WorkingDirectory string `json:"working_directory,omitempty"`
	} `json:"process,omitempty"`
	Registry *struct {
		Data *struct {
			Strings []string `json:"strings"`
			Type    string   `json:"type"`
		} `json:"data,omitempty"`
		Hive  string `json:"hive,omitempty"`
		Key   string `json:"key,omitempty"`
		Path  string `json:"path"`
		Value string `json:"value"`
	} `json:"registry,omitempty"`
	Related *struct {
		Hash []string `json:"hash,omitempty"`
		Ip   any      `json:"ip,omitempty"`
		User string   `json:"user,omitempty"`
	} `json:"related,omitempty"`
	SanitizedChannel string `json:"sanitized_channel,omitempty"`
	Source           *struct {
		Domain string `json:"domain"`
		Ip     string `json:"ip"`
		Port   int    `json:"port"`
	} `json:"source,omitempty"`
	Sysmon *struct {
		Dns struct {
			Status string `json:"status"`
		} `json:"dns"`
	} `json:"sysmon,omitempty"`
	User *struct {
		Domain string `json:"domain"`
		ID     string `json:"id"`
		Name   string `json:"name"`
	} `json:"user,omitempty"`
	Winlog struct {
		API          string `json:"api"`
		Channel      string `json:"channel"`
		ComputerName string `json:"computer_name"`
		EventData    struct {
			CallTrace         string `json:"CallTrace,omitempty"`
			Company           string `json:"Company,omitempty"`
			CreationUtcTime   string `json:"CreationUtcTime,omitempty"`
			Description       string `json:"Description,omitempty"`
			Details           string `json:"Details,omitempty"`
			EventType         string `json:"EventType,omitempty"`
			FileVersion       string `json:"FileVersion,omitempty"`
			GrantedAccess     string `json:"GrantedAccess,omitempty"`
			Hashes            string `json:"Hashes,omitempty"`
			Image             string `json:"Image,omitempty"`
			IntegrityLevel    string `json:"IntegrityLevel,omitempty"`
			IsExecutable      string `json:"IsExecutable,omitempty"`
			LogonGuid         string `json:"LogonGuid,omitempty"`
			LogonID           string `json:"LogonId,omitempty"`
			ParentUser        string `json:"ParentUser,omitempty"`
			ProcessGuid       string `json:"ProcessGuid,omitempty"`
			ProcessID         string `json:"ProcessId,omitempty"`
			Product           string `json:"Product,omitempty"`
			RuleName          string `json:"RuleName,omitempty"`
			Signature         string `json:"Signature,omitempty"`
			SignatureStatus   string `json:"SignatureStatus,omitempty"`
			Signed            string `json:"Signed,omitempty"`
			SourcePortName    string `json:"SourcePortName,omitempty"`
			SourceUser        string `json:"SourceUser,omitempty"`
			TargetFilename    string `json:"TargetFilename,omitempty"`
			TargetImage       string `json:"TargetImage,omitempty"`
			TargetObject      string `json:"TargetObject,omitempty"`
			TargetProcessGuid string `json:"TargetProcessGUID,omitempty"`
			TargetProcessID   string `json:"TargetProcessId,omitempty"`
			TargetUser        string `json:"TargetUser,omitempty"`
			TerminalSessionID string `json:"TerminalSessionId,omitempty"`
			User              string `json:"User,omitempty"`
			UtcTime           string `json:"UtcTime,omitempty"`
		} `json:"event_data"`
		EventID string `json:"event_id"`
		Opcode  string `json:"opcode"`
		Process struct {
			Pid    int `json:"pid"`
			Thread struct {
				ID int `json:"id"`
			} `json:"thread"`
		} `json:"process"`
		ProviderGuid string `json:"provider_guid"`
		ProviderName string `json:"provider_name"`
		RecordID     int    `json:"record_id"`
		Task         string `json:"task"`
		User         struct {
			Domain     string `json:"domain"`
			Identifier string `json:"identifier"`
			Name       string `json:"name"`
			Type       string `json:"type"`
		} `json:"user"`
		Version int `json:"version"`
	} `json:"winlog"`
}
