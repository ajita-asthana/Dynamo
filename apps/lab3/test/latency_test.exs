defmodule ClientDynamoTest do
    use ExUnit.Case
    doctest Dynamo

    import Emulation, only: [spawn: 2, send: 2]

    import Kernel,
      except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

    defp sendPutRequest(proc_name,message,node_name) do
        client = Dynamo.Client.new_client(node_name)
        {key,value,context} = message
        t1 = Emulation.now()
        {:ok, client} = Dynamo.Client.put(client, key, value, context)
        t2 = Emulation.now()
        t = t2 - t1
        t = Emulation.emu_to_millis(t)
        IO.puts("\n [#{proc_name}] PUT: At #{node_name} return [#{key},#{value},#{context}] TIME : #{t} \n")
    end

    defp sendGetRequest(proc_name,key,node_name) do
        client = Dynamo.Client.new_client(node_name)
        t1 = Emulation.now()
        {{value,ret_context},_} = Dynamo.Client.get(client,key)
        t2 = Emulation.now()
        t = t2 - t1
        t = Emulation.emu_to_millis(t)
        IO.puts("[#{proc_name}]GET: At #{node_name} return [#{key},#{value},#{ret_context}]")
        #IO.puts("Time Taken for #{inspect(:proc_name)} to get response : #{inspect(t)}")
    end

    def choose_random_node() do
        nodes = [:a, :b, :c, :d, :e, :f, :g, :h, :i, :j]
        Enum.random(nodes)
    end

    def generate_random_client_name() do
        clients = ["a", "b", "c", "d", "e","f", "g", "h", "i", "j","k", "l", "m", "n", "o"]
        random_client = Enum.random(clients)
        random_int = :rand.uniform(1000)
        client = :"#{random_client}#{random_int}"
    end

    test "Latency Test =>  When R+W = N" do
        IO.puts "TEST START"
        Emulation.init()
        Emulation.append_fuzzers([Fuzzers.delay(2)])
        view = %{a: true,b: true, c: true,d: true,e: true,f: true, g: true, h: true,i: true, j: true}
        key_value_map = %{}
        r = 1
        w = 2
        n = 3
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
            5_000 -> :ok
        end
        a = generate_random_client_name()
        spawn(a, fn -> sendPutRequest(a,{478,897,1},choose_random_node()) end)   #write
        receive do
        after
            1_000 -> :ok
        end
        a = generate_random_client_name()
        spawn(a, fn -> sendPutRequest(a,{478,897,2},choose_random_node()) end)   #write
        receive do
        after
            1_000 -> :ok
        end
        a = generate_random_client_name()
        spawn(a, fn -> sendPutRequest(a,{478,897,3},choose_random_node()) end)   #write
        receive do
        after
            1_000 -> :ok
        end
        a = generate_random_client_name()
        spawn(a, fn -> sendPutRequest(a,{478,897,4},choose_random_node()) end)   #write
        receive do
        after
            1_000 -> :ok
        end
        a = generate_random_client_name()
        spawn(a, fn -> sendPutRequest(a,{478,897,5},choose_random_node()) end)   #write
        receive do
        after
            1_000 -> :ok
        end
        a = generate_random_client_name()
        spawn(a, fn -> sendPutRequest(a,{478,897,6},choose_random_node()) end)   #write
        receive do
        after
            1_000 -> :ok
        end
        a = generate_random_client_name()
        spawn(a, fn -> sendPutRequest(a,{478,897,7},choose_random_node()) end)   #write
        receive do
        after
            1_000 -> :ok
        end
        a = generate_random_client_name()
        spawn(a, fn -> sendPutRequest(a,{478,897,8},choose_random_node()) end)   #write
        receive do
        after
            1_000 -> :ok
        end
        a = generate_random_client_name()
        spawn(a, fn -> sendPutRequest(a,{478,897,9},choose_random_node()) end)   #write
        receive do
        after
            1_000 -> :ok
        end
        a = generate_random_client_name()
        spawn(a, fn -> sendPutRequest(a,{478,897,10},choose_random_node()) end)   #write
        receive do
        after
            1_000 -> :ok
        end
        a = generate_random_client_name()
        spawn(a, fn -> sendPutRequest(a,{478,897,11},choose_random_node()) end)   #write
        receive do
        after
            1_000 -> :ok
        end
        a = generate_random_client_name()
        spawn(a, fn -> sendPutRequest(a,{478,897,12},choose_random_node()) end)   #write
        receive do
        after
            1_000 -> :ok
        end
        a = generate_random_client_name()
        spawn(a, fn -> sendPutRequest(a,{478,897,13},choose_random_node()) end)   #write
        receive do
        after
            1_000 -> :ok
        end
        a = generate_random_client_name()
        spawn(a, fn -> sendPutRequest(a,{478,897,14},choose_random_node()) end)   #write
        receive do
        after
            1_000 -> :ok
        end
        a = generate_random_client_name()
        spawn(a, fn -> sendPutRequest(a,{478,897,15},choose_random_node()) end)   #write
        receive do
        after
            1_000 -> :ok
        end
        a = generate_random_client_name()
        spawn(a, fn -> sendPutRequest(a,{478,897,16},choose_random_node()) end)   #write
        receive do
        after
            1_000 -> :ok
        end
        a = generate_random_client_name()
        spawn(a, fn -> sendPutRequest(a,{478,897,17},choose_random_node()) end)   #write
        receive do
        after
            1_000 -> :ok
        end
        a = generate_random_client_name()
        spawn(a, fn -> sendPutRequest(a,{478,897,18},choose_random_node()) end)   #write
        receive do
        after
            1_000 -> :ok
        end
        a = generate_random_client_name()
        spawn(a, fn -> sendPutRequest(a,{478,897,19},choose_random_node()) end)   #write
        receive do
        after
            1_000 -> :ok
        end
        a = generate_random_client_name()
        spawn(a, fn -> sendPutRequest(a,{478,897,20},choose_random_node()) end)   #write
        receive do
        after
            1_000 -> :ok
        end


        receive do
            after
            30000 -> :ok
        end
    after
        Emulation.terminate()
    end

end