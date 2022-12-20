defmodule StalenessTest1 do
    use ExUnit.Case
    doctest Dynamo

    import Emulation, only: [spawn: 2, send: 2]

    import Kernel,
      except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

    defp sendPutRequest(proc_name,message,node_name) do
        client = Dynamo.Client.new_client(node_name)
        {key,value,context} = message
        IO.puts("\n [#{proc_name}]PUT: At #{node_name} return [#{key},#{value},#{context}]\n")
        {:ok, client} = Dynamo.Client.put(client, key, value, context)
    end

    defp sendGetRequest(proc_name,key,node_name) do
        client = Dynamo.Client.new_client(node_name)
        
        {{value,ret_context},_} = Dynamo.Client.get(client,key)
        # IO.inspect(proc_name)
        IO.puts("[#{proc_name}] GET: At #{node_name} return [#{key},#{value},#{ret_context}]")
        # t1 = Emulation.now()
        # {:ok, proc_name} = Dynamo.Client.put(proc_name, key, value, context)
        # IO.puts("Write for contacted node: #{node_name}, client_node: #{proc_name} done ; [#{key},#{value},#{context}]")
        # t2 = Emulation.now()
        # t = t2 - t1
        # t = Emulation.emu_to_millis(t)
        #IO.puts("Time Taken for #{inspect(:proc_name)} to get response : #{inspect(t)}")
    end

    def choose_random_node() do
        nodes = [:a, :b, :c, :d, :e, :f, :g, :h, :i, :j]
        Enum.random(nodes)
    end
    test "Staleness Test =>  When R+W < N" do
        IO.puts "TEST START"
        Emulation.init()
        Emulation.append_fuzzers([Fuzzers.drop(0.0),Fuzzers.delay(2)])
        view = %{a: true,b: true, c: true,d: true,e: true,f: true, g: true, h: true,i: true, j: true}
        key_value_map = %{}
        r = 1
        w = 1
        n = 3
        gossip_timeout = 2000
        node_list = [:a, :b, :c, :d, :e, :f, :g, :h, :i, :j]
        key_range_data = %{a: [{0,99},{800,899},{900,999}],b: [{0,99},{100,199},{200,299}],c: [{0,99},{100,199},{200,299}],d: [{200,299},{300,399},{100,199}],e: [{200,299},{300,399},{400,499}],f: [{300,399},{400,499},{500,599}],g: [{400,499},{500,599},{600,699}],h: [{500,599},{600,699},{700,799}],i: [{600,699},{700,799},{800,899}],j: [{700,799},{800,899},{900,999}]}
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
        
        
        spawn(:c1, fn -> sendPutRequest(:c1,{478,9402,1},:f) end)   #write
        receive do
        after
            1_000 -> :ok
        end
        spawn(:c2, fn -> sendPutRequest(:c2,{478,940,2},:g) end)   #write
        receive do
        after
            1_000 -> :ok
        end
        spawn(:r3, fn -> sendGetRequest(:r3,478,:a) end)    #read
        spawn(:r4, fn -> sendGetRequest(:r4,478,:b) end)    #read
        spawn(:r5, fn -> sendGetRequest(:r5,478,:c) end)    #read
        spawn(:r6, fn -> sendGetRequest(:r6,478,:d) end)    #read
        spawn(:r7, fn -> sendGetRequest(:r7,478,:e) end)    #read
        spawn(:r8, fn -> sendGetRequest(:r8,478,:f) end)    #read
        spawn(:r9, fn -> sendGetRequest(:r9,478,:g) end)    #read
        spawn(:r10, fn -> sendGetRequest(:r10,478,:h) end)    #read
        spawn(:r11, fn -> sendGetRequest(:r11,478,:i) end)    #read
        spawn(:r12, fn -> sendGetRequest(:r12,478,:j) end)    #read
        
        receive do
        after
            1_000 -> :ok
        end
        spawn(:c3, fn -> sendPutRequest(:c3,{478,897,3},choose_random_node()) end)   #write
        receive do
        after
            1_000 -> :ok
        end
        spawn(:r13, fn -> sendGetRequest(:r13,478,choose_random_node()) end)    #read
        spawn(:r14, fn -> sendGetRequest(:r14,478,choose_random_node()) end)    #read
        spawn(:r15, fn -> sendGetRequest(:r15,478,choose_random_node()) end)    #read
        spawn(:r16, fn -> sendGetRequest(:r16,478,choose_random_node()) end)    #read
        spawn(:r17, fn -> sendGetRequest(:r17,478,choose_random_node()) end)    #read
        spawn(:r18, fn -> sendGetRequest(:r18,478,choose_random_node()) end)    #read
        spawn(:r19, fn -> sendGetRequest(:r19,478,choose_random_node()) end)    #read
        spawn(:r20, fn -> sendGetRequest(:r20,478,choose_random_node()) end)    #read
        spawn(:r21, fn -> sendGetRequest(:r21,478,choose_random_node()) end)    #read
        spawn(:r22, fn -> sendGetRequest(:r22,478,choose_random_node()) end)    #read
        receive do
        after
            1_000 -> :ok
        end
        spawn(:c4, fn -> sendPutRequest(:c4,{478,100,4},choose_random_node()) end)   #write
        receive do
        after
            1_000 -> :ok
        end
        spawn(:r23, fn -> sendGetRequest(:r23,478,choose_random_node()) end)    #read
        spawn(:r24, fn -> sendGetRequest(:r24,478,choose_random_node()) end)    #read
        spawn(:r25, fn -> sendGetRequest(:r25,478,choose_random_node()) end)    #read
        spawn(:r26, fn -> sendGetRequest(:r26,478,choose_random_node()) end)    #read
        spawn(:r27, fn -> sendGetRequest(:r27,478,choose_random_node()) end)    #read
        spawn(:r28, fn -> sendGetRequest(:r28,478,choose_random_node()) end)    #read
        spawn(:r29, fn -> sendGetRequest(:r29,478,choose_random_node()) end)    #read
        spawn(:r30, fn -> sendGetRequest(:r30,478,choose_random_node()) end)    #read
        spawn(:r31, fn -> sendGetRequest(:r31,478,choose_random_node()) end)    #read
        spawn(:r32, fn -> sendGetRequest(:r32,478,choose_random_node()) end)    #read
        receive do
        after
            1_000 -> :ok
        end
        spawn(:c5, fn -> sendPutRequest(:c5,{478,1100,5},choose_random_node()) end)   #write
        receive do
        after
            1_000 -> :ok
        end
        spawn(:r33, fn -> sendGetRequest(:r33,478,choose_random_node()) end)    #read
        spawn(:r34, fn -> sendGetRequest(:r34,478,choose_random_node()) end)    #read
        spawn(:r35, fn -> sendGetRequest(:r35,478,choose_random_node()) end)    #read
        spawn(:r36, fn -> sendGetRequest(:r36,478,choose_random_node()) end)    #read
        spawn(:r37, fn -> sendGetRequest(:r37,478,choose_random_node()) end)    #read
        spawn(:r38, fn -> sendGetRequest(:r38,478,choose_random_node()) end)    #read
        spawn(:r39, fn -> sendGetRequest(:r39,478,choose_random_node()) end)    #read
        spawn(:r40, fn -> sendGetRequest(:r40,478,choose_random_node()) end)    #read
        spawn(:r41, fn -> sendGetRequest(:r41,478,choose_random_node()) end)    #read
        spawn(:r42, fn -> sendGetRequest(:r42,478,choose_random_node()) end)    #read
        receive do
        after
            1_000 -> :ok
        end
        spawn(:c6, fn -> sendPutRequest(:c6,{478,3456,6},choose_random_node()) end)   #write
        receive do
        after
            1_000 -> :ok
        end
        spawn(:r43, fn -> sendGetRequest(:r43,478,choose_random_node()) end)    #read
        spawn(:r44, fn -> sendGetRequest(:r44,478,choose_random_node()) end)    #read
        spawn(:r45, fn -> sendGetRequest(:r45,478,choose_random_node()) end)    #read
        spawn(:r46, fn -> sendGetRequest(:r46,478,choose_random_node()) end)    #read
        spawn(:r47, fn -> sendGetRequest(:r47,478,choose_random_node()) end)    #read
        spawn(:r48, fn -> sendGetRequest(:r48,478,choose_random_node()) end)    #read
        spawn(:r49, fn -> sendGetRequest(:r49,478,choose_random_node()) end)    #read
        spawn(:r50, fn -> sendGetRequest(:r50,478,choose_random_node()) end)    #read
        spawn(:r51, fn -> sendGetRequest(:r51,478,choose_random_node()) end)    #read
        spawn(:r52, fn -> sendGetRequest(:r52,478,choose_random_node()) end)    #read
        receive do
        after
            1_000 -> :ok
        end
        spawn(:c7, fn -> sendPutRequest(:c7,{478,721,7},choose_random_node()) end)   #write
        receive do
        after
            1_000 -> :ok
        end
        spawn(:r53, fn -> sendGetRequest(:r53,478,choose_random_node()) end)    #read
        spawn(:r54, fn -> sendGetRequest(:r54,478,choose_random_node()) end)    #read
        spawn(:r55, fn -> sendGetRequest(:r55,478,choose_random_node()) end)    #read
        spawn(:r56, fn -> sendGetRequest(:r56,478,choose_random_node()) end)    #read
        spawn(:r57, fn -> sendGetRequest(:r57,478,choose_random_node()) end)    #read
        spawn(:r58, fn -> sendGetRequest(:r58,478,choose_random_node()) end)    #read
        spawn(:r59, fn -> sendGetRequest(:r59,478,choose_random_node()) end)    #read
        spawn(:r60, fn -> sendGetRequest(:r60,478,choose_random_node()) end)    #read
        spawn(:r61, fn -> sendGetRequest(:r61,478,choose_random_node()) end)    #read
        spawn(:r62, fn -> sendGetRequest(:r62,478,choose_random_node()) end)    #read
        receive do
        after
            1_000 -> :ok
        end
        spawn(:c8, fn -> sendPutRequest(:c8,{478,657,8},choose_random_node()) end)   #write
        receive do
        after
            1_000 -> :ok
        end
        spawn(:r63, fn -> sendGetRequest(:r63,478,choose_random_node()) end)    #read
        spawn(:r64, fn -> sendGetRequest(:r64,478,choose_random_node()) end)    #read
        spawn(:r65, fn -> sendGetRequest(:r65,478,choose_random_node()) end)    #read
        spawn(:r66, fn -> sendGetRequest(:r66,478,choose_random_node()) end)    #read
        spawn(:r67, fn -> sendGetRequest(:r67,478,choose_random_node()) end)    #read
        spawn(:r68, fn -> sendGetRequest(:r68,478,choose_random_node()) end)    #read
        spawn(:r69, fn -> sendGetRequest(:r69,478,choose_random_node()) end)    #read
        spawn(:r70, fn -> sendGetRequest(:r70,478,choose_random_node()) end)    #read
        spawn(:r71, fn -> sendGetRequest(:r71,478,choose_random_node()) end)    #read
        spawn(:r72, fn -> sendGetRequest(:r72,478,choose_random_node()) end)    #read
        receive do
        after
            1_000 -> :ok
        end
        spawn(:c9, fn -> sendPutRequest(:c9,{478,356,9},choose_random_node()) end)   #write
        receive do
        after
            1_000 -> :ok
        end
        spawn(:r73, fn -> sendGetRequest(:r73,478,choose_random_node()) end)    #read
        spawn(:r74, fn -> sendGetRequest(:r74,478,choose_random_node()) end)    #read
        spawn(:r75, fn -> sendGetRequest(:r75,478,choose_random_node()) end)    #read
        spawn(:r76, fn -> sendGetRequest(:r76,478,choose_random_node()) end)    #read
        spawn(:r77, fn -> sendGetRequest(:r77,478,choose_random_node()) end)    #read
        spawn(:r78, fn -> sendGetRequest(:r78,478,choose_random_node()) end)    #read
        spawn(:r79, fn -> sendGetRequest(:r79,478,choose_random_node()) end)    #read
        spawn(:r80, fn -> sendGetRequest(:r80,478,choose_random_node()) end)    #read
        spawn(:r81, fn -> sendGetRequest(:r81,478,choose_random_node()) end)    #read
        spawn(:r82, fn -> sendGetRequest(:r82,478,choose_random_node()) end)    #read

        receive do
        after
            1_000 -> :ok
        end
        spawn(:c10, fn -> sendPutRequest(:c8,{478,454,10},choose_random_node()) end)   #write
        receive do
        after
            1_000 -> :ok
        end
        spawn(:r83, fn -> sendGetRequest(:r83,478,choose_random_node()) end)    #read
        spawn(:r84, fn -> sendGetRequest(:r84,478,choose_random_node()) end)    #read
        spawn(:r85, fn -> sendGetRequest(:r85,478,choose_random_node()) end)    #read
        spawn(:r86, fn -> sendGetRequest(:r86,478,choose_random_node()) end)    #read
        spawn(:r87, fn -> sendGetRequest(:r87,478,choose_random_node()) end)    #read
        spawn(:r88, fn -> sendGetRequest(:r88,478,choose_random_node()) end)    #read
        spawn(:r89, fn -> sendGetRequest(:r89,478,choose_random_node()) end)    #read
        spawn(:r90, fn -> sendGetRequest(:r90,478,choose_random_node()) end)    #read
        spawn(:r91, fn -> sendGetRequest(:r91,478,choose_random_node()) end)    #read
        spawn(:r92, fn -> sendGetRequest(:r92,478,choose_random_node()) end)    #read




       receive do
        after
        30000 -> :ok
        end

        after
            Emulation.terminate()
        end

end