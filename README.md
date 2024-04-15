# TLA Path Generator

```shell
java -jar ~/Downloads/tla2tools.jar raft.tla -dump dot,colorize,actionlabels state
python3 ../raft_path_generator.py state.dot paths 12
go run -tags raft_tla . -a=raft_tla -i=$IID --ids=1,2,3 --logdir /Users/hank/protosim/tla/raft-tla
```