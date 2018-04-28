package main

import (
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"

	"gopkg.in/mgo.v2/bson"
	
	"github.com/erh/mongonet"
)


// ----------

type HostPort struct {
	Host string
	Port int
}

func (hp *HostPort) Address() string {
	return fmt.Sprintf("%s:%d", hp.Host, hp.Port)
}

type HostAlias struct {
	remote HostPort
	local HostPort
}

type HostMapping []HostAlias

func getReplacement(s string, mapping HostMapping) (string, bool) {
	var err error

	for _, alias := range mapping {
		if strings.HasPrefix(s, alias.remote.Host) {
			rest := s[len(alias.remote.Host):]
			port := int64(27017)
			if len(rest) > 0 {
				if rest[0] != ':' {
					continue
				}
				rest = rest[1:]
				port, err = strconv.ParseInt(rest, 10, 32)
				if err != nil {
					continue
				}
			}
			if int(port) != alias.remote.Port {
				continue
			}
			return alias.local.Address(), true
		}
	}

	return "", false
}

func fixHostNamesValue(elem interface{}, mapping HostMapping) interface{} {
	switch val := elem.(type) {
	case string:
		n, found := getReplacement(val, mapping)
		if found {
			return n
		}
	case bson.D:
		return FixHostNames(val, mapping)
	case []interface{}:
		for j, elem2 := range val {
			val[j] = fixHostNamesValue(elem2, mapping)
		}
		return elem
	}
	return elem
}

func FixHostNames(doc bson.D, mapping HostMapping) bson.D {
	for i, elem := range doc {
		doc[i].Value = fixHostNamesValue(elem.Value, mapping)
	}
	return doc
}


// ------

type fixReplicaSetStrings struct {
	mapping HostMapping
}

func (fixer *fixReplicaSetStrings) fix(doc mongonet.SimpleBSON) (mongonet.SimpleBSON, error) {
	d, err := doc.ToBSOND()
	if err != nil {
		return doc, err
	}
	d = FixHostNames(d, fixer.mapping)
	return mongonet.SimpleBSONConvert(d)
}

func (fixer *fixReplicaSetStrings) InterceptMongoToClient(m mongonet.Message) (mongonet.Message, error) {
	switch mm := m.(type) {
	case *mongonet.ReplyMessage:
		if len(mm.Docs) != 1 {
			return mm, mongonet.NewStackErrorf("too many docs for ReplyMessage")
		}

		n, err := fixer.fix(mm.Docs[0])
		mm.Docs[0] = n

		return mm, err
	case *mongonet.CommandReplyMessage:
		// TODO: check if i can ignore metadata, i think so
		
		n, err := fixer.fix(mm.CommandReply)
		mm.CommandReply = n

		return mm, err
	case *mongonet.MessageMessage:
		var bodySection *mongonet.BodySection = nil
		for _, section := range mm.Sections {
			if bs, ok := section.(*mongonet.BodySection); ok {
				if bodySection != nil {
					return mm, mongonet.NewStackErrorf("OP_MSG should not have more than one body section!  Second body section: %v", bs)
				}
				bodySection = bs
			} else {
				// MongoDB 3.6 does not support anything other than body sections in replies
				return mm, mongonet.NewStackErrorf("OP_MSG replies with sections other than a body section are not supported!")
			}
		}

		if bodySection == nil {
			return mm, mongonet.NewStackErrorf("OP_MSG should have a body section!")
		}

		n, err := fixer.fix(bodySection.Body)
		bodySection.Body = n

		return mm, err
	default:
		return m, mongonet.NewStackErrorf("bad response for reply code %d", m.Header().OpCode)
	}


	return m, nil
}

// ----------

func fixIsMasterQuery(query bson.D) bson.D {

	fixed := bson.D{}
	
	for _, elem := range query {
		
		if elem.Name == "client" {
			continue
		}

		fixed = append(fixed, elem)			
	}

	return fixed
}

// ----------

type MyFactory struct {
	mapping HostMapping
}

func (myf *MyFactory) NewInterceptor(ps *mongonet.ProxySession) (mongonet.ProxyInterceptor, error) {
	return &MyInterceptor{ps, myf.mapping}, nil
}

type MyInterceptor struct {
	ps *mongonet.ProxySession
	mapping HostMapping
}

func (myi *MyInterceptor) commandICareAbout(cmd string) bool {
	cmd = strings.ToLower(cmd)
	//fmt.Println(cmd)
	return cmd == "ismaster" || cmd == "replsetgetstatus"
}

func (myi *MyInterceptor) InterceptClientToMongo(m mongonet.Message) (mongonet.Message, mongonet.ResponseInterceptor, error) {
	//fmt.Printf("InterceptClientToMongo got a %T\n", m)
	switch mm := m.(type) {
	case *mongonet.QueryMessage:
		if !mongonet.NamespaceIsCommand(mm.Namespace) {
			return m, nil, nil
		}

		query, err := mm.Query.ToBSOND() // TODO: switch to new bson library
		if err != nil || len(query) == 0 {
			// let mongod handle error message
			return m, nil, nil
		}

		cmdName := query[0].Name
		if !myi.commandICareAbout(cmdName) {
			return m, nil, nil
		}

		query = fixIsMasterQuery(query)
		temp, err := mongonet.SimpleBSONConvert(query)
		if err != nil {
			return mm, nil, err
		}
		mm.Query = temp
		
		return mm, &fixReplicaSetStrings{myi.mapping}, nil
	case *mongonet.CommandMessage:
		if !myi.commandICareAbout(mm.CmdName) {
			return mm, nil, nil
		}
		return m, &fixReplicaSetStrings{myi.mapping}, nil
	case *mongonet.MessageMessage:
		var bodySection *mongonet.BodySection = nil
		
		for _, section := range mm.Sections {
			if bodySec, ok := section.(*mongonet.BodySection); ok {
				if bodySection != nil {
					return mm, nil, fmt.Errorf("OP_MSG contains more than one body section")
				}
				
				bodySection = bodySec
			}
		}
		
		if bodySection == nil {
			return mm, nil, fmt.Errorf("OP_MSG does not contain a body section")
		}
		
		cmd, err := bodySection.Body.ToBSOND()
		if err != nil {
			return mm, nil, mongonet.NewStackErrorf("can't parse body section bson: %v", err)
		}
		cmdName := cmd[0].Name

		if !myi.commandICareAbout(cmdName) {
			return m, nil, nil
		}
		
		return m, &fixReplicaSetStrings{myi.mapping}, nil
	}

	return m, nil, nil
}

func (myi *MyInterceptor) Close() {
}
func (myi *MyInterceptor) TrackRequest(mongonet.MessageHeader) {
}
func (myi *MyInterceptor) TrackResponse(mongonet.MessageHeader) {
}

func (myi *MyInterceptor) CheckConnection() error {
	return nil
}

func (myi *MyInterceptor) CheckConnectionInterval() time.Duration {
	return 0
}

// ----------


func runProxy(p mongonet.Proxy) {
	err := p.Run()
	if err != nil {
		panic(err)
	}

}

func main() {
	bindHost := flag.String("host", "127.0.0.1", "what to bind to")
	bindPortStart := flag.Int("portStart", 30000, "what port to start at")
	
	seedList := flag.String("seedList", "", "")

	flag.Parse()

	if *seedList == "" {
		panic(fmt.Errorf("need a seedList"))
	}

	hosts := strings.Split(*seedList, ",")
	proxyConfigs := []mongonet.ProxyConfig{}
	mapping := []HostAlias{}
		
	for idx, h := range hosts {
		bindPort := *bindPortStart + idx
		remotePort := 27017

		pcs := strings.Split(h, ":")
		if len(pcs) == 1 {
			// no-op
		} else if len(pcs) == 2 {
			h = pcs[0]
			pp, err := strconv.ParseInt(pcs[1], 10, 32)
			if err != nil {
				panic(fmt.Errorf("cannot parse port %s", pcs[1]))
			}
			remotePort = int(pp)
		} else {
			panic(fmt.Errorf("bad hostname %s", h))
		}
		
		fmt.Printf("%s:%d -->> %s:%d\n", *bindHost, bindPort, h, remotePort)
		
		pc := mongonet.NewProxyConfig(*bindHost, bindPort, h, remotePort)
		pc.MongoSSL = true

		proxyConfigs = append(proxyConfigs, pc)
		mapping = append(mapping, HostAlias{HostPort{h, remotePort}, HostPort{*bindHost, bindPort}})
	}

	for _, pc := range proxyConfigs {
		pc.InterceptorFactory = &MyFactory{mapping}
		
		p := mongonet.NewProxy(pc)
		go runProxy(p)
	}

	for {
		time.Sleep(100000)
	}
}
