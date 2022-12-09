defmodule ClientDynamoTest do
    use ExUnit.Case
    doctest Dynamo

    import Emulation, only: [spawn: 2, send: 2]

    import Kernel,
      except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

    defp sendPutRequest(proc_name,message,node_name) do
        IO.puts("here...")
        proc_name = Dynamo.Client.new_client(node_name)
        IO.puts("there....")
        {key,value,context} = message
        t1 = Emulation.now()
        IO.puts("t1 : #{t1}")
        {:ok, proc_name} = Dynamo.Client.put(proc_name, key, value, context)
        # IO.inspect(proc_name)
        
        t2 = Emulation.now()
        t = t2 - t1
        t = Emulation.emu_to_millis(t)
        IO.puts("Put Request completed. contacted node:#{node_name} [#{key},#{value},#{context}], time: #{t}")
        #IO.puts("Time Taken for #{inspect(:proc_name)} to get response : #{inspect(t)}")
    end

    defp sendGetRequest(proc_name,key,node_name) do
        proc_name = Dynamo.Client.new_client(node_name)
        
        {{value,ret_context},_} = Dynamo.Client.get(proc_name,key)
        IO.inspect(proc_name)
        IO.puts("GET for contacted node: #{node_name},and key: #{key} done ; [#{key},#{value},#{ret_context}]")
        # t1 = Emulation.now()
        # {:ok, proc_name} = Dynamo.Client.put(proc_name, key, value, context)
        # IO.puts("Write for contacted node: #{node_name}, client_node: #{proc_name} done ; [#{key},#{value},#{context}]")
        # t2 = Emulation.now()
        # t = t2 - t1
        # t = Emulation.emu_to_millis(t)
        #IO.puts("Time Taken for #{inspect(:proc_name)} to get response : #{inspect(t)}")
    end

    test "Latency Test =>  When R+W = N" do
        IO.puts "TEST START"
        Emulation.init()
        Emulation.append_fuzzers([Fuzzers.delay(2)])
        view = %{a: true,b: true, c: true,d: true,e: true,f: true, g: true, h: true,i: true, j: true}
        key_value_map = %{}
        r = 2
        w = 3
        n = 5
        gossip_timeout = 2000
        node_list = [:a, :b, :c, :d, :e, :f, :g, :h, :i, :j]
        key_range_data = %{a: [{0,99} , {800,899},{900,999}],b: [{0,99},{100,199},{200,299}],c: [{0,99},{100,199},{200,299}],d: [{200,299},{300,399},{100,199}],e: [{200,299},{300,399},{400,499}],f: [{300,399},{400,499},{500,599}],g: [{400,499},{500,599},{600,699}],h: [{500,599},{600,699},{700,799}],i: [{600,699},{700,799},{800,899}],j: [{700,799},{800,899},{900,999}]}
        base_config = Dynamo.new_configuration(view,key_value_map, r,w,n, node_list,key_range_data, gossip_timeout)
        spawn(:a, fn -> Dynamo.start_Dynamo(base_config) end)
        spawn(:b, fn -> Dynamo.start_Dynamo(base_config) end)
        spawn(:c, fn -> Dynamo.start_Dynamo(base_config) end)
        spawn(:d, fn -> Dynamo.start_Dynamo(base_config) end)
        spawn(:e, fn -> Dynamo.start_Dynamo(base_config) end)
        spawn(:f, fn -> Dynamo.start_Dynamo(base_config) end)
        spawn(:g, fn -> Dynamo.start_Dynamo(base_config) end)
        spawn(:h, fn -> Dynamo.start_Dynamo(base_config) end)
        spawn(:i, fn -> Dynamo.start_Dynamo(base_config) end)
        spawn(:j, fn -> Dynamo.start_Dynamo(base_config) end)
        receive do
        after
            1_000 -> :ok
        end
        
        spawn(:c1, fn -> sendPutRequest(:c1,{10,9402,1},:a) end)
        spawn(:c2, fn -> sendPutRequest(:c2,{10,9402,2},:b) end)
        spawn(:c3, fn -> sendPutRequest(:c3,{10,9402,3},:c) end)
        spawn(:c4, fn -> sendPutRequest(:c4,{10,9402,4},:d) end)

        spawn(:c5, fn -> sendGetRequest(:c5,10,:f) end)
    after
        Emulation.terminate()
    end

end