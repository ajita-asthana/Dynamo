defmodule FailureLatencyTest do
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
        IO.puts("[#{proc_name}] PUT,#{t}")
    end

    defp sendGetRequest(proc_name,key,node_name) do
        client = Dynamo.Client.new_client(node_name)
        t1 = Emulation.now()
        {{value,ret_context},_} = Dynamo.Client.get(client,key)
        t2 = Emulation.now()
        t = t2 - t1
        t = Emulation.emu_to_millis(t)
        IO.puts("[#{proc_name}]GET,#{t}")
        #IO.puts("Time Taken for #{inspect(:proc_name)} to get response : #{inspect(t)}")
    end

    defp sendStopRequest(contact_node,kill_node) do
        client = Dynamo.Client.new_client(contact_node)
        Dynamo.Client.stopProcess(client,kill_node)
        # t = Emulation.emu_to_millis(t)
        IO.puts("[#{contact_node}] #{kill_node} stops/fails.")
        #IO.puts("Time Taken for #{inspect(:proc_name)} to get response : #{inspect(t)}")
    end

    def choose_random_node() do
        nodes = [:a, :b, :c, :d, :e, :f, :g, :h, :i, :j]
        Enum.random(nodes)
    end
    test "Failure Latency Test =>  When R+W < N" do
        Emulation.init()
        Emulation.append_fuzzers([Fuzzers.delay(2)])
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
        
        
    spawn(:c0, fn -> sendPutRequest(:c0,{369,135,1},choose_random_node()) end)  # Write at a random node
    receive do
    after
        5_000 -> :ok
    end
    spawn(:r1, fn -> sendStopRequest(:a,:c) end)    # kill node :c
    receive do
    after
        1_000 -> :ok
    end
    # spawn(:r2, fn -> sendStopRequest(:a,:d) end)    # kill node :b
    # receive do
    # after
    #     1_000 -> :ok
    # end
    spawn(:c1, fn -> sendGetRequest(:c1,369,choose_random_node()) end)
    spawn(:c2, fn -> sendGetRequest(:c2,369,choose_random_node()) end)
    spawn(:c3, fn -> sendGetRequest(:c3,369,choose_random_node()) end)
    spawn(:c4, fn -> sendGetRequest(:c4,369,choose_random_node()) end)
    spawn(:c5, fn -> sendGetRequest(:c5,369,choose_random_node()) end)
    spawn(:c6, fn -> sendGetRequest(:c6,369,choose_random_node()) end)
    spawn(:c7, fn -> sendGetRequest(:c7,369,choose_random_node()) end)
    spawn(:c8, fn -> sendGetRequest(:c8,369,choose_random_node()) end)
    spawn(:c9, fn -> sendGetRequest(:c9,369,choose_random_node()) end)
    spawn(:c10, fn -> sendGetRequest(:c10,369,choose_random_node()) end)
    spawn(:c11, fn -> sendGetRequest(:c11,369,choose_random_node()) end)
    spawn(:c12, fn -> sendGetRequest(:c12,369,choose_random_node()) end)
    spawn(:c13, fn -> sendGetRequest(:c13,369,choose_random_node()) end)
    spawn(:c14, fn -> sendGetRequest(:c14,369,choose_random_node()) end)
    spawn(:c15, fn -> sendGetRequest(:c15,369,choose_random_node()) end)
    spawn(:c16, fn -> sendGetRequest(:c16,369,choose_random_node()) end)
    spawn(:c17, fn -> sendGetRequest(:c17,369,choose_random_node()) end)
    spawn(:c18, fn -> sendGetRequest(:c18,369,choose_random_node()) end)
    spawn(:c19, fn -> sendGetRequest(:c19,369,choose_random_node()) end)
    spawn(:c20, fn -> sendGetRequest(:c20,369,choose_random_node()) end)
    spawn(:c21, fn -> sendGetRequest(:c21,369,choose_random_node()) end)
    spawn(:c22, fn -> sendGetRequest(:c22,369,choose_random_node()) end)
    spawn(:c23, fn -> sendGetRequest(:c23,369,choose_random_node()) end)
    spawn(:c24, fn -> sendGetRequest(:c24,369,choose_random_node()) end)
    spawn(:c25, fn -> sendGetRequest(:c25,369,choose_random_node()) end)
    spawn(:c26, fn -> sendGetRequest(:c26,369,choose_random_node()) end)
    spawn(:c27, fn -> sendGetRequest(:c27,369,choose_random_node()) end)
    spawn(:c28, fn -> sendGetRequest(:c28,369,choose_random_node()) end)
    spawn(:c29, fn -> sendGetRequest(:c29,369,choose_random_node()) end)
    spawn(:c30, fn -> sendGetRequest(:c30,369,choose_random_node()) end)
    spawn(:c31, fn -> sendGetRequest(:c31,369,choose_random_node()) end)
    spawn(:c32, fn -> sendGetRequest(:c32,369,choose_random_node()) end)
    spawn(:c33, fn -> sendGetRequest(:c33,369,choose_random_node()) end)
    spawn(:c34, fn -> sendGetRequest(:c34,369,choose_random_node()) end)
    spawn(:c35, fn -> sendGetRequest(:c35,369,choose_random_node()) end)
    spawn(:c36, fn -> sendGetRequest(:c36,369,choose_random_node()) end)
    spawn(:c37, fn -> sendGetRequest(:c37,369,choose_random_node()) end)
    spawn(:c38, fn -> sendGetRequest(:c38,369,choose_random_node()) end)
    spawn(:c39, fn -> sendGetRequest(:c39,369,choose_random_node()) end)
    spawn(:c40, fn -> sendGetRequest(:c40,369,choose_random_node()) end)
    spawn(:c41, fn -> sendGetRequest(:c41,369,choose_random_node()) end)
    spawn(:c42, fn -> sendGetRequest(:c42,369,choose_random_node()) end)
    spawn(:c43, fn -> sendGetRequest(:c43,369,choose_random_node()) end)
    spawn(:c44, fn -> sendGetRequest(:c44,369,choose_random_node()) end)
    spawn(:c45, fn -> sendGetRequest(:c45,369,choose_random_node()) end)
    spawn(:c46, fn -> sendGetRequest(:c46,369,choose_random_node()) end)
    spawn(:c47, fn -> sendGetRequest(:c47,369,choose_random_node()) end)
    spawn(:c48, fn -> sendGetRequest(:c48,369,choose_random_node()) end)
    spawn(:c49, fn -> sendGetRequest(:c49,369,choose_random_node()) end)
    spawn(:c50, fn -> sendGetRequest(:c50,369,choose_random_node()) end)
    spawn(:c51, fn -> sendGetRequest(:c51,369,choose_random_node()) end)
    spawn(:c52, fn -> sendGetRequest(:c52,369,choose_random_node()) end)
    spawn(:c53, fn -> sendGetRequest(:c53,369,choose_random_node()) end)
    spawn(:c54, fn -> sendGetRequest(:c54,369,choose_random_node()) end)
    spawn(:c55, fn -> sendGetRequest(:c55,369,choose_random_node()) end)
    spawn(:c56, fn -> sendGetRequest(:c56,369,choose_random_node()) end)
    spawn(:c57, fn -> sendGetRequest(:c57,369,choose_random_node()) end)
    spawn(:c58, fn -> sendGetRequest(:c58,369,choose_random_node()) end)
    spawn(:c59, fn -> sendGetRequest(:c59,369,choose_random_node()) end)
    spawn(:c60, fn -> sendGetRequest(:c60,369,choose_random_node()) end)
    spawn(:c61, fn -> sendGetRequest(:c61,369,choose_random_node()) end)
    spawn(:c62, fn -> sendGetRequest(:c62,369,choose_random_node()) end)
    spawn(:c63, fn -> sendGetRequest(:c63,369,choose_random_node()) end)
    spawn(:c64, fn -> sendGetRequest(:c64,369,choose_random_node()) end)
    spawn(:c65, fn -> sendGetRequest(:c65,369,choose_random_node()) end)
    spawn(:c66, fn -> sendGetRequest(:c66,369,choose_random_node()) end)
    spawn(:c67, fn -> sendGetRequest(:c67,369,choose_random_node()) end)
    spawn(:c68, fn -> sendGetRequest(:c68,369,choose_random_node()) end)
    spawn(:c69, fn -> sendGetRequest(:c69,369,choose_random_node()) end)
    spawn(:c70, fn -> sendGetRequest(:c70,369,choose_random_node()) end)
    spawn(:c71, fn -> sendGetRequest(:c71,369,choose_random_node()) end)
    spawn(:c72, fn -> sendGetRequest(:c72,369,choose_random_node()) end)
    spawn(:c73, fn -> sendGetRequest(:c73,369,choose_random_node()) end)
    spawn(:c74, fn -> sendGetRequest(:c74,369,choose_random_node()) end)
    spawn(:c75, fn -> sendGetRequest(:c75,369,choose_random_node()) end)
    spawn(:c76, fn -> sendGetRequest(:c76,369,choose_random_node()) end)
    spawn(:c77, fn -> sendGetRequest(:c77,369,choose_random_node()) end)
    spawn(:c78, fn -> sendGetRequest(:c78,369,choose_random_node()) end)
    spawn(:c79, fn -> sendGetRequest(:c79,369,choose_random_node()) end)
    spawn(:c80, fn -> sendGetRequest(:c80,369,choose_random_node()) end)
    spawn(:c81, fn -> sendGetRequest(:c81,369,choose_random_node()) end)
    spawn(:c82, fn -> sendGetRequest(:c82,369,choose_random_node()) end)
    spawn(:c83, fn -> sendGetRequest(:c83,369,choose_random_node()) end)
    spawn(:c84, fn -> sendGetRequest(:c84,369,choose_random_node()) end)
    spawn(:c85, fn -> sendGetRequest(:c85,369,choose_random_node()) end)
    spawn(:c86, fn -> sendGetRequest(:c86,369,choose_random_node()) end)
    spawn(:c87, fn -> sendGetRequest(:c87,369,choose_random_node()) end)
    spawn(:c88, fn -> sendGetRequest(:c88,369,choose_random_node()) end)
    spawn(:c89, fn -> sendGetRequest(:c89,369,choose_random_node()) end)
    spawn(:c90, fn -> sendGetRequest(:c90,369,choose_random_node()) end)
    spawn(:c91, fn -> sendGetRequest(:c91,369,choose_random_node()) end)
    spawn(:c92, fn -> sendGetRequest(:c92,369,choose_random_node()) end)
    spawn(:c93, fn -> sendGetRequest(:c93,369,choose_random_node()) end)
    spawn(:c94, fn -> sendGetRequest(:c94,369,choose_random_node()) end)
    spawn(:c95, fn -> sendGetRequest(:c95,369,choose_random_node()) end)
    spawn(:c96, fn -> sendGetRequest(:c96,369,choose_random_node()) end)
    spawn(:c97, fn -> sendGetRequest(:c97,369,choose_random_node()) end)
    spawn(:c98, fn -> sendGetRequest(:c98,369,choose_random_node()) end)
    spawn(:c99, fn -> sendGetRequest(:c99,369,choose_random_node()) end)
    spawn(:c100, fn -> sendGetRequest(:c100,369,choose_random_node()) end)




       receive do
        after
        30000 -> :ok
        end

        after
            Emulation.terminate()
        end

end