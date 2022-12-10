defmodule Dynamo do
    import Emulation, only: [send: 2, timer: 2, now: 0, whoami: 0]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  require Fuzzers
  require Logger
  require List
  require Tuple

  #Configuration of the Dynamo :
  #Parameters :
  #Read : No. of Reads Required = 2 (For this example)
  #Write : No of Writes Required = 1 (For this example) Highly available System :-P
  # N = Lets set N = 2
  defstruct(
      view: %{}, #Keeps the List of all the Nodes and checks if they are alive.
                 #So, this Map contains the ProcessName -> true , if the ProcessName is communicating with the current Process.

      hash_map: %{}, #This is the KeyValue Store present at the given Node
      read_param: nil, #The R Parameter. Lets Keep R = 2
      write_param: nil, # The W Parameter. Lets Keep W = 1. Highly available Key Value Store.
      n_param: nil, # The N Parameter N = 2
      node_list: nil, # The list of the Nodes in the order the come.
      key_range_data: %{}, # Data of all the ranges of Keys contained by each Process.
      gossip_timer: nil,
      gossip_timeout: nil,
      merkle_tree_map: %{}
  )
  @spec new_configuration(
          map(),
          map(),
          non_neg_integer(), #Actually Greater than 0
          non_neg_integer(), #Actually Greater than 0
          non_neg_integer(), #Actually Greater than 0
          [atom()],
          map(),
          non_neg_integer() 
        ) :: %Dynamo{}
  def new_configuration(
      view,
      hash_map,
      read_param,
      write_param,
      n_param,
      node_list,
      key_range_data,
      gossip_timeout
      ) do
    %Dynamo{
      view: view,
      hash_map: hash_map,
      read_param: read_param,
      write_param: write_param,
      n_param: n_param,
      node_list: node_list,
      key_range_data: key_range_data,
      gossip_timeout: gossip_timeout
    }
  end

  ######################################################################################################################################################
  # UTILITY FUNCTIONS
  # Lets put the Utility Functions Here
  ######################################################################################################################################################

  @spec is_proc_alive(%Dynamo{},atom()) :: boolean()
  def is_proc_alive(state,procName) do
    {:ok, {ans, time}} = Map.fetch(state.view,procName)
    if ans == true do
      true
    else 
      false #Always remember will have to change it
    end
  end


  #Returns the List of all the Nodes whoose view is set as true.
  @spec fetch_true_list(%Dynamo{}) :: [atom()]
  def fetch_true_list(state) do
    # READ: state.view = [[true, 12000], [true, 22000], [true, 30000]]
    # This will check if first item in element == true and add it to view
    list = Enum.filter(state.node_list, fn proc -> is_proc_alive(state,proc) == true end)
    #IO.puts(" List: #{inspect(list)}")
    list
  end

  @spec correct_range({any, any}, any) :: boolean
  def correct_range(tup, key) do
    {low, high} = tup
    if key >= low and key <= high do
      true
    else 
      false
    end
  end


  def check_valid_range(key_range,key) do
    if key_range == [] do
      false
    else 
      [head|tail] = key_range
      if correct_range(head,key) == true do
        true
      else 
        check_valid_range(tail,key)
      end
    end
  end

  @spec is_valid_proc(%Dynamo{},atom(),non_neg_integer) :: boolean()
  def is_valid_proc(state,proc,key) do
    {:ok , key_ranges} = Map.fetch(state.key_range_data,proc)
    check_valid_range(key_ranges,key)
  end

  def extract_elements(list,n) do
    if n==0 do
      []
    else
      [head|tail] = list
      ans = [head] ++ extract_elements(tail, n - 1)
    end
  end

  def extract_first_n_elements(list,element, n) do
    if list==[] do
      []
    else 
      [head | tail] = list
      if head == element do
        ans = extract_elements(list,n)
      else 
        extract_first_n_elements(tail,element,n)
      end
    end
  end

  def write_to_state(state,request) do
    {key,value,context} = request
    new_hash_map = Map.put(state.hash_map,key,{value,context})
    state = %{state|hash_map: new_hash_map}
    #IO.puts("######### WRITE REQUEST ###############")
    #IO.puts("Name of Process : #{inspect(whoami())}")
    #IO.puts("Printing the State : ")
    #IO.puts("#{inspect(state)}")
    state
  end

  @spec fetch_key_list(non_neg_integer(), %Dynamo{}) :: [atom()]
  def fetch_key_list(key, state) do
    # Returns the List of N Healthy Nodes Corresponding to the Key
    # First Find the Key with :true and return
    # READ: state.view = [[true, 12000], [true, 22000], [true, 30000]]
    # This will check if N first items so if N = 2 => state.view = [[true, 12000], [true, 22000],]
    list = fetch_true_list(state)
    list = list ++ list
    key_list = Enum.filter(list, fn proc -> is_valid_proc(state,proc,key) == true end)
    [head|tail] = key_list
    key_list = extract_first_n_elements(list,head,state.n_param)
  end

  @spec check_eligibility([atom()],atom()) :: boolean()
  def check_eligibility(list, proc_name) do
    if list == [] do
      false
    else 
      [head|tail] = list
      if head==proc_name do
        true
      else
        check_eligibility(tail,proc_name)
      end
    end
  end

  @spec set_all_node_working(%Dynamo{}) :: %Dynamo{}
  def set_all_node_working(state) do
    #new_view = Enum.map(state.view, fn k -> %{List.first(Tuple.to_list(k)) => [true, Emulation.now()]} end)
    new_view = Enum.map(state.view, fn {k,v} -> {k, {true,Emulation.emu_to_millis(Emulation.now())}} end)
    new_view = Map.new(new_view)
    state = %{state| view: new_view}
    #IO.puts("State : #{inspect(state)}")
    state
  end

  def create_list_of_merkle_trees(start_index,end_index,curr_index) do
    if curr_index > end_index do
      []
    else
      new_merkle = MerkleTree.new(curr_index)
      new_merkle = %{new_merkle| isLeaf: true}
      new_merkle = %{new_merkle| range: {curr_index,curr_index}}
      str = "[#{curr_index},0,0]"
      new_hash = Base.encode16(:crypto.hash(:sha256, str))
      new_merkle = %{new_merkle| hash1: new_hash}
      list = [new_merkle] ++ create_list_of_merkle_trees(start_index,end_index,curr_index + 1)
    end
  end

  def generate_merkle_tree_from_list(list,ans) do
    if list==[] do
      ans
    else
      if length(list)== 1 do
        ans = ans ++ list
        ans
      else
        [first_tree|tail] = list
        [second_tree|rem_tail] = tail
        new_merkle = MerkleTree.new(1)
        new_merkle = %{new_merkle| left_child: first_tree}
        new_merkle = %{new_merkle| right_child: second_tree}
        hash_calc = Base.encode16(:crypto.hash(:sha256,first_tree.hash1 <> second_tree.hash1))
        new_merkle = %{new_merkle| hash1: hash_calc}
        {range_1_low,_} = first_tree.range
        {_,range_2_high} = second_tree.range
        final_range = {range_1_low,range_2_high}
        new_merkle = %{new_merkle| range: final_range}
        new_merkle = %{new_merkle| isLeaf: false}
        ans = ans ++ [new_merkle]  
        generate_merkle_tree_from_list(rem_tail,ans)
      end
    end
  end

  def generate_merkle_tree_from_list(list) do
    if length(list) == 1 do
      list
    else
      list = generate_merkle_tree_from_list(list,[])
      generate_merkle_tree_from_list(list)
    end
  end

  def create_merkle_tree_from_tuple(tup) do
    {start_index,end_index} = tup
    list = create_list_of_merkle_trees(start_index,end_index,start_index)
    ans = generate_merkle_tree_from_list(list)
    ans
  end


  def create_Merkle_tree_map(list,map) do
    if list==[] do
      map
    else
      [head|tail] = list
      new_merkle = create_merkle_tree_from_tuple(head)
      new_map = Map.put(map,head,new_merkle)
      if whoami() == :a do
        #IO.puts("New Map : #{inspect(new_map[head])}")
      end
      create_Merkle_tree_map(tail,new_map)
    end
  end

  @spec start_Dynamo(%Dynamo{})::no_return()
  def start_Dynamo(state) do
    #Time to start Dynamo.
    #For the First Time Will Listen to Get/Put Request
    state = set_all_node_working(state)
    list_of_key_ranges = state.key_range_data[whoami()]
    # my_merkle_tree_map = create_Merkle_tree_map(list_of_key_ranges,%{})
    # if whoami() == :a do
    #   my_tree = my_merkle_tree_map[{0,99}]
    #   [head|tail] = my_tree
    #   IO.puts("Merkle Tree Map myline : #{inspect(head.range)}")
    # end
    # state = %{state| merkle_tree_map: my_merkle_tree_map}
    t = Emulation.timer(state.gossip_timeout,:gossip_timer)
    state = %{state| gossip_timer: t}
    listen_client_request(state)
  end

  def eligible_put_request(state,request) do
    {key,value,context} = request
    if state.hash_map[key] == nil do
      true
    else
      {:ok, {val,c}} = Map.fetch(state.hash_map,key)
      if c >= context do
        false
      else 
        true
      end
    end
  end

  def broadcast_request_to_others(state, message, mylist) do
    me  = whoami()
    mylist
    |> Enum.filter(fn pid -> pid != me end)
    |> Enum.map(fn pid -> send(pid, message) end)
  end

  def mark_removed_process(view,proc_name) do
    new_map = Map.put(view,proc_name,{false,Emulation.emu_to_millis(Emulation.now())})
    new_map
  end

  def get_new_key_range(tup,list) do
    if list == [] do
      #Add the tuple
      [tup]
    else
      [head|tail] = list
      {start_head,end_head} = head
      {start_tup,end_tup} = tup
      if start_head==start_tup and end_head==end_tup do
        #IO.inspect("Reached here Once")
        list
      else
        [head] ++ get_new_key_range(tup,tail)
      end
    end
  end

  def get_latest_key_range(state,tup,list) do
    if list == [] do
      state
    else
      [head|tail] = list
      #IO.puts("list : #{inspect(list)}")
      get_new_list = state.key_range_data[head]
      new_key_range_data = get_new_key_range(tup,get_new_list)
      #IO.puts("new_key_range_data : #{inspect(new_key_range_data)}")
      new_map  = Map.put(state.key_range_data,head,new_key_range_data)
      state = %{state | key_range_data: new_map}
      state = get_latest_key_range(state,tup,tail)
      state
    end   
  end

  def get_correct_range(state,tup) do
    {start_range,end_range} = tup
    list = fetch_key_list(start_range,state)
    state = get_latest_key_range(state,tup,list) #Insert the tuple on every list
    state
  end

  def correct_key_range(state,list) do
    if list == [] do
      state
    else
      [head|tail] = list
      state = get_correct_range(state,head)
      state = correct_key_range(state,tail)
      state
    end
  end

  def reconcile_key_range(state,proc_name) do
    list = state.key_range_data[proc_name]
    if list == nil do
      state
    else
      state = correct_key_range(state,list)
      state
    end
  end

  def mark_process_dead(state,proc_name) do
    new_view = mark_removed_process(state.view,proc_name)
    state = %{state| view: new_view}
    state = reconcile_key_range(state,proc_name)
    #IO.puts("reconciled Key Range : #{inspect(state)}")
    new_map = Map.put(state.key_range_data,proc_name,[])
    state = %{state| key_range_data: new_map}
    state
  end

  def mark_process_alive(state,keyList) do
    if keyList == [] do
      state
    else
      [head|tail] = keyList
      new_view = Map.put(state.view,head,{true,Emulation.emu_to_millis(Emulation.now())})
      state = %{state|view: new_view}
      state = mark_process_alive(state,tail)
      state
    end
  end

  def get_proc_at_index(list,index) do
    [head|tail] = list
    if index == 0 do
      head
    else
      get_proc_at_index(tail,index - 1)
    end
  end

  def get_random_process(state) do
    true_list = fetch_true_list(state)
    num = :rand.uniform(length(true_list))
    proc_name = get_proc_at_index(true_list,num - 1)
    if proc_name == whoami() do
      get_random_process(state)
    else
      proc_name
    end
  end

  #These are two HashMaps
  def reconcile_views(first_view,second_view,list_of_node) do
    if list_of_node == [] do
      first_view
    else
      [head|tail] = list_of_node
      {first_view_is_alive,first_view_time} = first_view[head]
      {second_view_is_alive,second_view_time} = second_view[head]
      if first_view_time < second_view_time do
        new_tup = {second_view_is_alive,second_view_time}
        new_view = Map.put(first_view,head,new_tup)
        reconcile_views(new_view,second_view,tail)
      else
        reconcile_views(first_view,second_view,tail)
      end
    end
  end

  def reconcile_all_failed_process(state,list) do
    if list == [] do
      state
    else
      [head|tail] = list
      get_val = state.view[head]
      {is_process_alive,time} = get_val
      if is_process_alive == false do
        state = mark_process_dead(state, head)
        reconcile_all_failed_process(state,tail)
      else
        reconcile_all_failed_process(state,tail)
      end
    end
  end

  def listen_to_write_request(state,count,request) do
    {sender,key,value,context,keyList} = request
    if count == 0 do
      send(sender,:ok)
      listen_client_request(state)
    else
      #Hmm Listen to Requests
      #Dont take any more Client Requests
      #However take requests from the Other Nodes
      receive do
        :gossip_timer -> 
                        proc_name = get_random_process(state) 
                        message = {:gossip_view}
                        send(proc_name,message)
                        t = Emulation.timer(state.gossip_timeout,:gossip_timer)
                        state = %{state| gossip_timer: t}
                        listen_to_write_request(state,count,request)
        
        {sender,{:gossip_view}} ->   
                        message = {:gossip_view,state.view}
                        send(sender,message)
                        listen_to_write_request(state,count,request)

        {_,{:gossip_view,other_view}} ->
                          Emulation.cancel_timer(state.gossip_timer)
                          reconciled_view = reconcile_views(state.view,other_view,state.node_list)
                          state = %{state| view: reconciled_view}
                          state = reconcile_all_failed_process(state,state.node_list)
                          t = Emulation.timer(state.gossip_timeout,:gossip_timer)
                          state = %{state| gossip_timer: t}
                          listen_to_write_request(state,count,request)




        {_,:get_state} -> IO.puts("State of #{inspect(whoami())} : #{inspect(state)}")
                         listen_to_write_request(state,count,request)
        {_,{:stop,proc_name}} -> 
                        if proc_name == whoami() do
                          #IO.puts("#{inspect(whoami())} Dies Now :-(")
                        else
                            state = mark_process_dead(state,proc_name)
                            listen_to_write_request(state,count,request)
                        end


      {sender, %Message.PutRequest{
        key: key,
        value: value,
        context: context
        }} -> if eligible_put_request(state,{key,value,context}) do
                state = write_to_state(state,{key,value,context})
                state = mark_process_alive(state,[sender])
                message = %Message.PutResponse{
                  key: key,
                  context: context,
                  success: true
                }
                send(sender,message)
                listen_to_write_request(state,count,request)
              else
                message = %Message.PutResponse{
                  key: key,
                  context: context,
                  success: false
                }
                send(sender,message)
                listen_to_write_request(state,count,request)
              end
              


      {sender, %Message.PutResponse{
        key: key,
        context: context,
        success: success
      }} -> 
        # IO.puts("[dynamo] put response received from #{sender} with success #{success}")
          state = mark_process_alive(state,[sender])
          if success == true do
            listen_to_write_request(state,count - 1,request)
          else
            listen_to_write_request(state, count,request)
          end
      

      {sender, %Message.GetRequest{
        key: key
      }} ->
              state = mark_process_alive(state,[sender])
              if state.hash_map[key] == nil do
                message = %Message.GetResponse{
                  key: key,
                  value: nil,
                  context: nil
                }
                send(sender,message)
                listen_to_write_request(state, count,request)
              else 
                {:ok,{val,cont}} = Map.fetch(state.hash_map,key)
                message = %Message.GetResponse{
                  key: key,
                  value: val,
                  context: cont
                }
                send(sender,message)
                listen_to_write_request(state, count,request)
              end 
              
      

      {sender, %Message.GetResponse{
        key: key,
        value: value,
        context: context
      }} -> 
            state = mark_process_alive(state,[sender])
            listen_to_write_request(state, count,request)
      
      end
    end
  end

  def transition_to_write_mode(state,request) do
    #Read the Request
    {sender,key,value,context,keyList} = request
    message = %Message.PutRequest{
      key: key,
      value: value,
      context: context
    }
    if eligible_put_request(state,{key,value,context}) do
      broadcast_request_to_others(state,message,keyList)
      new_hash_map = Map.put(state.hash_map,key,{value,context})
      state = %{state| hash_map: new_hash_map}
      listen_to_write_request(state,state.write_param - 1,request)
    else
      listen_client_request(state)
    end
  end

  def listen_to_read_request(state,request,response,read_count) do
    if read_count == 0 do
      #Prepare Response and send it
      {sender,key,keyList} = request
      #IO.puts("Sending the Response : #{inspect(response)}")
      send(sender,response)
      listen_client_request(state)
    else
      receive do
        :gossip_timer -> 
                        proc_name = get_random_process(state) 
                        message = {:gossip_view}
                        send(proc_name,message)
                        t = Emulation.timer(state.gossip_timeout,:gossip_timer)
                        state = %{state| gossip_timer: t}
                        listen_to_read_request(state,request,response,read_count)
        
        {sender,{:gossip_view}} ->   
                        message = {:gossip_view,state.view}
                        send(sender,message)
                        listen_to_read_request(state,request,response,read_count)

        {_,{:gossip_view,other_view}} ->
                          Emulation.cancel_timer(state.gossip_timer)
                          reconciled_view = reconcile_views(state.view,other_view,state.node_list)
                          state = %{state| view: reconciled_view}
                          state = reconcile_all_failed_process(state,state.node_list)
                          t = Emulation.timer(state.gossip_timeout,:gossip_timer)
                          state = %{state| gossip_timer: t}
                          listen_to_read_request(state,request,response,read_count)

        {_,:get_state} -> IO.puts("State of #{inspect(whoami())} : #{inspect(state)}")
                        listen_to_read_request(state,request,response,read_count)

        {_,{:stop,proc_name}} -> 
                  if proc_name == whoami() do
                    #IO.puts("#{inspect(whoami())} Dies Now :-(")
                  else
                      state = mark_process_dead(state,proc_name)
                      listen_to_read_request(state,request,response,read_count)
                  end


        {sender, %Message.PutRequest{
          key: key,
          value: value,
          context: context
         }} ->
          if eligible_put_request(state,{key,value,context}) do
            state = write_to_state(state,{key,value,context})
            state = mark_process_alive(state,[sender])
            message = %Message.PutResponse{
              key: key,
              context: context,
              success: true
            }
            send(sender,message)
            listen_to_read_request(state,request,response,read_count)
          else
            message = %Message.PutResponse{
              key: key,
              context: context,
              success: false
            }
            send(sender,message)
            listen_to_read_request(state,request,response,read_count)
          end
        
        {sender, %Message.PutResponse{
          key: key,
          context: context
        }} -> 
          state = mark_process_alive(state,[sender])  
          listen_to_read_request(state,request,response,read_count)

        {sender, %Message.GetRequest{
          key: key
        }} ->  
        # IO.puts("\n1. Read request received at #{whoami()}")
          state = mark_process_alive(state,[sender])
        if state.hash_map[key] == nil do
          message = %Message.GetResponse{
            key: key,
            value: nil,
            context: nil
          }
          send(sender,message)
          listen_to_read_request(state,request,response,read_count)
        else 
          # IO.puts("\nRead request received at #{whoami()}")
          # IO.inspect(state.hash_map)
          {:ok,{val,cont}} = Map.fetch(state.hash_map,key)
          message = %Message.GetResponse{
            key: key,
            value: val,
            context: cont
          }
          send(sender,message)
          listen_to_read_request(state,request,response,read_count)
        end 

        {sender, %Message.GetResponse{
          key: key,
          value: value,
          context: context
          }} -> 
            state = mark_process_alive(state,[sender])
            if value == nil do
              listen_to_read_request(state,request,response,read_count - 1)
            else
              {response_value,response_context} = response
              if response_context < context do
                response = {value,context}
                listen_to_read_request(state,request,response,read_count - 1)
              else
                listen_to_read_request(state,request,response,read_count - 1)
              end

            end
      end
    end 
  end

  def transition_to_read_mode(state,request) do
    {sender,key,keyList} = request
    message = %Message.GetRequest{
      key: key
    }
    broadcast_request_to_others(state,message,keyList)
    if state.hash_map[key] == nil do
      response = {nil,nil}
      listen_to_read_request(state,request,response,state.read_param - 1)
    else
      {:ok,response} = Map.fetch(state.hash_map,key)
      listen_to_read_request(state,request,response,state.read_param - 1)
    end

  end

  @spec listen_client_request(%Dynamo{})::no_return()
  def listen_client_request(state) do
    receive do
      :gossip_timer -> 
                        proc_name = get_random_process(state) 
                        message = {:gossip_view}
                        send(proc_name,message)
                        t = Emulation.timer(state.gossip_timeout,:gossip_timer)
                        state = %{state| gossip_timer: t}
                        listen_client_request(state)
                        
        
        {sender,{:gossip_view}} ->   
                        message = {:gossip_view,state.view}
                        send(sender,message)
                        listen_client_request(state)

        {_,{:gossip_view,other_view}} ->
                          Emulation.cancel_timer(state.gossip_timer)
                          reconciled_view = reconcile_views(state.view,other_view,state.node_list)
                          # if whoami() == :f do
                          #   IO.puts("Reconciled View of F : #{inspect(reconciled_view)}")
                          # end
                          state = %{state| view: reconciled_view}
                          state = reconcile_all_failed_process(state,state.node_list)
                          t = Emulation.timer(state.gossip_timeout,:gossip_timer)
                          state = %{state| gossip_timer: t}
                          listen_client_request(state)

          
      {_,:get_state} -> IO.puts("State of #{inspect(whoami())} : #{inspect(state)}")
                      listen_client_request(state)

      {_,{:stop,proc_name}} -> 
      #IO.puts("Stop Message here : #{inspect(proc_name)}")
      if proc_name == whoami() do
          #IO.puts("#{inspect(whoami())} Dies Now :-(")
      else
        #will see what to do
        state = mark_process_dead(state,proc_name)  
        listen_client_request(state)
      end

      {sender, {:put, key, value, context}} ->
        #We need a function for Redirection.
        keyList = fetch_key_list(key,state)
        # IO.puts("\nput request recvd at #{whoami()}")
        # IO.inspect(keyList)
        node_eligible = check_eligibility(keyList,whoami())
        # IO.puts("\nnode eligible: #{node_eligible}")
        if node_eligible == true do
          # IO.puts("\nis eligible.")
          request = {sender,key,value,context,keyList}
          transition_to_write_mode(state,request)
        else
          
          [head|tail] = keyList
          message = {:redirect,head}
          send(sender,message)
          # IO.puts("\n Not  eligible, redirecting to #{head}")
          #IO.puts("Message: #{inspect(message)}")
          listen_client_request(state)
        end


      {sender, {:get, key}} ->
        # IO.puts("Got Get Request From Client State of Node: #{inspect(state)}")
        keyList = fetch_key_list(key,state)
        #IO.puts("Returns from here")
        node_eligible = check_eligibility(keyList,whoami())
        if node_eligible == true do
          # A Get Request from the Client
          # Lets go to ReadMode and Listen to ReadRequests.
          # Once we get R responses we will construct the response and will transfer it to the Client.
          # We will return the Client the Value with the Highest Context Value.
          request = {sender,key,keyList}
          #IO.puts("Inspecting Request : #{inspect(request)}")
          transition_to_read_mode(state,request)
        else
          #We are not the eligible one.
          #Just return a Redirection Response to the Client so that it can contact the Correct Node
          [head|tail] = keyList
          message = {:redirect,head}
          send(sender,message)
          listen_client_request(state)
        end


      #Time For Response from Other Nodes :
       {sender, %Message.PutRequest{
        key: key,
        value: value,
        context: context
       }} ->
        #What to do if other Nodes have more updated Version of the PutRequest
        #IO.puts("#{inspect(whoami())} recieved an request for key : #{inspect(key)}")
        if eligible_put_request(state,{key,value,context}) do
          state = write_to_state(state,{key,value,context})
          state = mark_process_alive(state,[sender])
          message = %Message.PutResponse{
            key: key,
            context: context,
            success: true
          }
          send(sender,message)
          listen_client_request(state)
        else
          message = %Message.PutResponse{
            key: key,
            context: context,
            success: false
          }
          send(sender,message)
          listen_client_request(state)
        end
        

       {sender, %Message.PutResponse{
        key: key,
        context: context
       }} -> 
        #This put is already done.
        #No need to count or anything else
        state = mark_process_alive(state,[sender])  
        listen_client_request(state)

       {sender, %Message.GetRequest{
        key: key
       }} ->
        state = mark_process_alive(state,[sender])
        if state.hash_map[key] == nil do
          message = %Message.GetResponse{
            key: key,
            value: nil,
            context: nil
          }
          send(sender,message)
          listen_client_request(state)
        else 
          {:ok,{val,cont}} = Map.fetch(state.hash_map,key)
          message = %Message.GetResponse{
            key: key,
            value: val,
            context: cont
          }
          send(sender,message)
          listen_client_request(state)
        end 
        

       {sender, %Message.GetResponse{
        key: key,
        value: value,
        context: context
       }} -> 
        #Got some response 
        state = mark_process_alive(state,[sender])
        listen_client_request(state)

       #Leave Some space for Merkle-Tree Reconcilation :


       # Make Room for Gossip-Protocols :


      m -> #Any Error Messages  :
          IO.puts("Recieved unknown message #{inspect{m}}")

    end
  end

end


defmodule Dynamo.Client do
    import Emulation, only: [send: 2]

    import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

    alias __MODULE__
    @enforce_keys [:node]
    defstruct(node: nil)

    @spec new_client(atom()) :: %Client{node: atom()}
    def new_client(member) do
      %Client{node: member}
    end

    @spec put(%Client{}, atom(), atom(), any()) :: {:ok, %Client{}}
    def put(client, key, value, context) do
      other_node = client.node
      message = {:put, key, value, context}
      send(other_node, message)
      receive do
        {_, {:redirect, other_node}} ->
          put(%{client | node: other_node}, key, value, context)

        {_, :ok} ->
          #IO.puts("Client Recieved an Confirmation")
          {:ok, client}

        m -> IO.puts("Client Recieved unknown message #{inspect{m}}")

      end
    end

    @spec get(%Client{}, atom()) :: {:ok, %Client{}}
    def get(client, key) do
      other_node = client.node
      message = {:get, key}
      send(other_node, message)
      receive do
        {_, {:redirect, other_node}} ->
            get(%{client| node: other_node}, key)

        {_, {value, staleness}} -> {{value, staleness}, client}


      end
    end

    @spec update_node(%Client{}, atom()) :: {:ok, atom()}
    def update_node(client, new_node) do
      #The Client will update the Contacted Node :
      #Will be useful for testing when the Node Fails
      client = %{client|node: new_node}
      client
    end

    @spec stop_process(%Client{},atom()) :: :no_return
    def stop_process(client,proc_name) do
      other_node = client.node
      message = {:stop,proc_name}
      IO.puts("Name of Failed  Process : #{inspect(message)}")
      send(proc_name,message)
      send(other_node,message)
    end


end
