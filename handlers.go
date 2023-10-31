package main

var handlers = map[string]func([]Data) Data{
	"PING": ping,
}

func ping(args []Data) Data {
	var reply Data
	switch len(args) {
	case 0:
		{
			reply.dataType = datatypes[SIMPLE_STRING]
			reply.simpleStr = "PONG"
		}
	case 1:
		{
			reply.dataType = datatypes[BULK_STRING]
			reply.bulkStr = args[0].bulkStr
		}
	default:
		{
			reply.dataType = datatypes[ERROR]
			reply.errorMsg = "ERR wrong number of arguments for 'ping' command"
		}
	}
	return reply
}
