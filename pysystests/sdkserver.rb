#!/usr/bin/env ruby

require 'rubygems'
require 'eventmachine'
require 'couchbase'
require 'yajl'

$client_map = Hash.new

module RequestHandler 
  def receive_data(data)
    data = parse_data(data)
    if $client_map.has_key?(data["bucket"]) == false
        url = "http://"+data["cb_ip"]+":"+data["cb_port"]+"/pools/default/"
        new_client = Couchbase.new url 
        new_client.quiet = false 
        $client_map[data["bucket"]] = new_client
    end

    res = exec_request(data)
    # return response to worker
    send_data(res)
  end
end

def parse_data(data)
    parser = Yajl::Parser.new
    data = parser.parse(data)
    return data
end
    
def exec_request(data)
    if data['command'] == 'set'
        return do_set(data)
    end

    if data['command'] == 'mset'
        return do_mset(data)
    end

    if data['command'] == 'mget'
        return do_mget(data)
    end

    if data['command'] == 'get'
        return do_get(data)
    end
    if data['command'] == 'delete'
        return do_delete(data)
    end
    if data['command'] == 'mdelete'
        return do_mdelete(data)
    end
    if data['command'] == 'query'
        return do_query(data)
    end

end

def do_set(data)

    key = data['args'][0]
    ttl = data['args'][1]
    flags = data['args'][2]
    value = data['args'][3]

    return $client_map[data["bucket"]].set key, value, :flags => flags, :ttl => ttl 
end

def do_mset(data)


    for kv in data['args']
        key = kv[0]
        ttl = kv[1]
        flags = kv[2]
        value = kv[3]
        $client_map[data["bucket"]].set key, value, :flags => flags, :ttl => ttl 
    end
end



def do_mget(data)

    keys = data['args']
    errors = nil
    num_gets = 0
    begin
        res = $client_map[data["bucket"]].get keys
        if res != nil
            res = res.compact  # remove nil
            num_gets = res.length
        end
    rescue Couchbase::Error::NotFound => e
        errors = e
    end

    
    return {"gets" => num_gets, "errors" => errors} 
end



def do_get(data)
    res = nil
    begin
        key = data['args'][0]
        res = $client_map[data["bucket"]].get key
    rescue Couchbase::Error::NotFound => e
        res = e
    end

    return res 
end

def do_delete(data)
    res = nil
    begin
        key = data['args'][0]
        res = $client_map[data["bucket"]].delete key
    rescue Couchbase::Error::NotFound => e
        res = e
    end

    return res
end

def do_mdelete(data)
    res = nil
    begin
        for key in data['args']
            res = $client_map[data["bucket"]].delete key
        end
    rescue Couchbase::Error::NotFound => e
        res = e
    end

    return res
end


def do_query(data)
    design_doc_name = data['args'][0]
    view_name = data['args'][1]
    bucket = data['args'][2]
    params = data['args'][3]

    #TODO: ruby sdk has removed views code!
    return nil
end


EventMachine::run do
  host = '127.0.0.1'
  port = 50009 
  EventMachine::start_server host, port, RequestHandler 
  puts "Started Ruby RequestHandler on #{host}:#{port}..."
end

