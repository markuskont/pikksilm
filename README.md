# pikksilm

Pikksilm is Estonian for *looking glass*. It is a streaming log correlation tool that enriches NDR events with EDR data. The tool is written entirely in Go.

It was developed for and used by Estonian-Georgian Blue Team during Locked Shields 2022 execution. The idea is to correlate Sysmon events 1 (Process creation) and 3 (Network connection seen), and to use those correlations enrich corresponding network capture sessions in Arkime and Suricata.

## Core formula

Sysmon Event ID 1 and 3 can be correlated using the `entity_id` value. Information from network event can then be used to generate Community ID that's used by Arkime WISE for lookups. Suricata events have to be streamed through pikksilm to achieve same enrichment. Optionally, only network events without correlated process creation can also be forwarded for enrichment. This will still provide process info without parent process fields.

## Gettings started

### Winlogbeat

Firstly, make sure you have a functioning winlogbeat setup that forwards all logs to a redis instance. Please refer to [`third_party`](/third_party/elastic) folder for configuration examples. Please note that at the time only ECS formatted messages are supported, which requires winlogbeat 7 with client-side processor script. Also note that configuration files in this repository are meant to be simple examples for setting up lab environments. Production setup would require specific winlogbeat event IDs to be forwarded from Logstash to Redis instead.

Winlogbeat must be set up on all monitored Windows machines.

### Sysmon

Download [Sysmon](https://docs.microsoft.com/en-us/sysinternals/downloads/sysmon) from sysinternals. You also need a configuration with XML schema version that corresponds to sysmon version. I recommend [Sysmon modular](https://github.com/olafhartong/sysmon-modular#pre-grenerated-configurations). Then go to folder that contains downloaded sysmon binaries and configuration XML and use following command to install it.

```
Sysmon64.exe -accepteula -i sysmonconfig.xml
```

Or you can use the following command to update config to existing installation.

```
Sysmon64.exe -c sysmonconfig.xml
```

This needs to be done on all monitored Windows machines.

### Building pikksilm

Binary tagged releases can be downloaded from Releases section. However, latest commit can easily be built from source, provided you already have a working Go setup. Simply clone the repo and build a binary.

```
go build -o pikksilm .
```

Pikksilm has no external dependencies. Simply copy the binary to the system that will be running it.

### Redis

All event streaming is done through redis. That includes:
* Winlogbeat events;
* WISE enrichments for Arkime;
* Suricata events;

Easiest way to spin up Redis is with a docker container. Again, this is meant for lab setups. Adjust according to your needs. Password protected setups are supported.

```
docker run --name redis-pikksilm -p 6379:6379 -d --restart=unless-stopped redis
```

Use `redis-cli` for debugging.

```
docker exec -ti redis-filebeat redis-cli -n 0
```

To verify that winlogbeat events are properly forwarded, do `LLEN` to see number of messages under a key. Ensure that pikksilm is not running, as it would consume all the events. And thus key would be missing. Note that `winlogbeat` is redis key value that can be reconfigured in pikksilm config file.

```
LLEN winlogbeat
```

To verify that correlations are sent correctly, do `KEYS` query over redis database that will be receiving the WISE enrichment data. Note that `-n 1` corresponds to different database than in winlogbeat event example. That's because by default we separate winlogbeat events, correlations and network-only data to different databases to avoid mixed data behind same community ID keys. Those databases are configurable, so adjust accordingly.

```
docker exec -ti redis-filebeat redis-cli -n 1
```

```
KEYS *
```

Likewise, to verify network-only enrichments, connect to corresponding database and verify that keys exist. Note that by default this is disabled and needs to be with `wise.connections.enabled`.

```
docker exec -ti redis-filebeat redis-cli -n 2
```

```
KEYS *
```

### Arkime WISE

Ensure Arkime is already configured to enrich sessions via WISE. Refer to Arkime official documentation on how to do that.

Add custom WISE lookup type for community ID to `config.ini`.

```
[wise-types]
communityid=communityId
```

Add custom view to `config.ini`.

```
[custom-views]
sysmon=title:Sysmon correlation;require:sysmon;fields:sysmon.parentprocessname,sysmon.parentprocesspid,sysmon.processname,sysmon.processpid,sysmon.username,sysmon.hostname,sysmon.hostip,sysmon.hostmac
```

Add the following two sections to `wise.ini`. Note that both are required even if you only configure correlations, as either defines only half of the required database fields.

```
[redis:sysmon]
url=redis://:password@127.0.0.1:6379/1
redisURL=redis://:password@127.0.0.1:6379/1
tags=correlation
type=communityid
format=json
template=1:%key%
keyPath=network.community_id
fields=field:sysmon.processname;db:sysmon.processname;kind:termfield;friendly:Process Name;shortcut:process.name\nfield:sysmon.username;db:sysmon.username;kind:termfield;friendly:User;shortcut:user.name\nfield:sysmon.hostname;db:sysmon.hostname;kind:termfield;friendly:Host Name;shortcut:host.name\nfield:sysmon.processpid;db:sysmon.processpid;kind:integer;friendly:Process PID;shortcut:process.pid\nfield:sysmon.hostip;db:sysmon.hostip;kind:ip;friendly:Host IP-s;shortcut:host.ip\nfield:sysmon.hostmac;db:sysmon.hostmac;kind:termfield;friendly:Host MAC;shortcut:host.mac
redisMethod=lpop
```

```

[redis:sysmonevent1]
url=redis://:password@127.0.0.1:6379/2
redisURL=redis://:password@127.0.0.1:6379/2
tags=connection
type=communityid
format=json
template=1:%key%
keyPath=network.community_id
fields=field:sysmon.parentprocessname;db:sysmon.parentprocessname;kind:termfield;friendly:Parent Process Name;shortcut:process.parent.name\nfield:sysmon.parentprocesspid;db:sysmon.parentprocesspid;kind:termfield;friendly:Parent Process PID;shortcut:process.parent.pid\nfield:sysmon.processmd5;db:sysmon.processmd5;kind:termfield;friendly:Process MD5;shortcut:hash.md5\nfield:sysmon.processargs;db:sysmon.processargs;kind:textfield;friendly:Process Arguments;shortcut:process.command_line\nfield:sysmon.processintlevel;db:sysmon.processintlevel;kind:termfield;friendly:Process Integrity Level;shortcut:winlog.event_data.IntegrityLevel\n
redisMethod=lpop
```

Note that a very recent version of Arkime is required for this to work!

### Configuring pikksilm

Updated command line documentation can be found under `run` subcommand help reference.

```
❯ pikksilm run --help
This command enriches network events by correlating sysmon command and network
  events via community ID enrichment.

  pikksilm run

Usage:
  pikksilm run [flags]

Flags:
  -h, --help                                      help for run
      --sysmon-redis-db int                       Redis database for sysmon consumer.
      --sysmon-redis-host string                  Redis host to consume sysmon from. (default "localhost:6379")
      --sysmon-redis-key string                   Redis key for winlogbeat messages. (default "winlogbeat")
      --sysmon-redis-password string              Password for sysmon redis instance. Empty value disables authentication.
      --wise-connection-enabled                   Enable forwarding of raw network events with not ID 1 correlation.
      --wise-connections-redis-db int             Redis database for WISE connections. (default 2)
      --wise-connections-redis-host string        Redis host output connections for WISE. Only event ID 3. (default "localhost:6379")
      --wise-connections-redis-password string    Password for WISE Redis connections. Empty value disables authentication.
      --wise-correlations-redis-db int            Redis database for WISE correlations. (default 1)
      --wise-correlations-redis-host string       Redis host output correlations for WISE. Event ID 1 + 3. (default "localhost:6379")
      --wise-correlations-redis-password string   Password for WISE Redis correlations. Empty value disables authentication.
      --work-dir string                           Working directory. Used for persistence. Pikksilm needs write access (default "/var/lib/pikksilm")
      --workers-sysmon-consume int                Number of workers for sysmon consume and JSON decode. (default 2)
      --workers-sysmon-correlate int              Number of workers for sysmon correlation. (default 2)

Global Flags:
      --config string   config file (default is $HOME/.pikksilm.yaml)
```

Note the last line `--config`. All arguments can be passed via YAML, JSON or TOML config file instead. For example, use `config` subcommand to generate the file.

```
pikksilm --config example.yaml config
```

File suffix defines the format. A TOML file can be generated as such, if you prefer that.

```
pikksilm --config example.toml config
```

You should find the following structure in generated file. Please update params to point pikksilm to correct redis instances. Different redis servers could be used.

```
general:
    work_dir: /var/lib/pikksilm
sysmon:
    redis:
        db: 0
        host: localhost:6379
        key: winlogbeat
        password: ""
wise:
    connections:
        enabled: false
        redis:
            db: 2
            host: localhost:6379
            password: ""
    correlations:
        redis:
            db: 1
            host: localhost:6379
            password: ""
workers:
    sysmon:
        consume: 2
        correlate: 2

```

Ensure that workind directory defined under `general.work_dir` exists and is writable to user executing the tool. You should create a dedicated service account for production setup. Otherwise, simply reconfigure `general.work_dir` to point towards freely chosen directory that is writable to your user. Do also note that pikksilm will attempt to read persistence data on startup. **If you have already executed pikksilm as another user, then it might crash.** That's because persistence files might belong to old user, resulting in permissions error. This is a common mistake when testing on a server as `root` and then trying to set up a service account for systemd.

```
grep pikksilm /etc/passwd || useradd --system -d /var/lib/pikksilm pikksilm
mkdir -p /var/lib/pikksilm
chown pikksilm /var/lib/pikksilm
```

Finally, execute the `run` subcommand while pointing pikksilm to config.

```
pikksilm --config example.yaml run
```

You should see statistics in stdout if everything went well.

```
INFO[38355] stream report                                 cmd_bucket_moves=0 count=23151 count_command=6 count_network=29 dropped=0 enriched=0 guid_missing=0 invalid_event=0 net_events_popped=0 net_events_stored=29 task=correlate worker=7
INFO[38355] stream report                                 cmd_bucket_moves=0 count=7745 count_command=3 count_network=64 dropped=0 enriched=0 guid_missing=0 invalid_event=0 net_events_popped=0 net_events_stored=64 task=correlate worker=6
INFO[38370] stream report                                 cmd_bucket_moves=0 count=8491 count_command=3 count_network=299 dropped=0 enriched=0 guid_missing=0 invalid_event=0 net_events_popped=0 net_events_stored=299 task=correlate worker=0
INFO[38370] stream report                                 cmd_bucket_moves=4 count=76387 count_command=6 count_network=339 dropped=0 enriched=32 guid_missing=0 invalid_event=0 net_events_popped=0 net_events_stored=311 task=correlate worker=1
INFO[38370] stream report                                 cmd_bucket_moves=0 count=145 count_command=4 count_network=26 dropped=0 enriched=0 guid_missing=0 invalid_event=0 net_events_popped=0 net_events_stored=26 task=correlate worker=5
INFO[38370] stream report                                 cmd_bucket_moves=0 count=265 count_command=4 count_network=78 dropped=0 enriched=0 guid_missing=0 invalid_event=0 net_events_popped=0 net_events_stored=78 task=correlate worker=4
INFO[38370] stream report                                 cmd_bucket_moves=0 count=113512 count_command=2 count_network=253 dropped=0 enriched=0 guid_missing=0 invalid_event=0 net_events_popped=0 net_events_stored=253 task=correlate worker=3
```

### Suricata

**Coming soon**. Suricata EVE enrichment is supported in backend. However, unlike Arkime which can be enriched through WISE, Suricata events need to be streamed through pikksilm. This was implemented in initial prototype but the feature has not yet been reworked for new multi-worker setup. Stay posted or use a older version from Releases. Do note that older versions use different configuration syntax that does not reflect this documentation.

### Systemd service

Place service file into `/etc/systemd/system` and execute `systemctl daemon-reload`. **Please ensure that file paths match your setup**.

```
[Unit]
Description=Pikksilm EDR to NDR correlator and enrichment
After=network.target

[Service]
Type=simple
Restart=on-failure
EnvironmentFile=-/etc/pikksilm.env
ExecStart=/usr/local/bin/pikksilm --config /etc/pikksilm.yaml run
WorkingDirectory=/
User=pikksilm
Group=daemon

[Install]
WantedBy=multi-user.target
```

Then enable and start the service.

```
systemctl enable pikksilm.service
systemctl start pikksilm.service
```

Logs are exposed via journald.

```
journalctl -u pikksilm.service --follow --output cat
```

## How to contribute

* try it out :)
* for any questions or problems, please make a issue
* ...
