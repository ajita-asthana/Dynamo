defmodule Dynamo do
  @moduledoc """
  An implementation of the Raft consensus protocol.
  """
  # Shouldn't need to spawn anything from this module, but if you do
  # you should add spawn to the imports.
  import Emulation, only: [send: 2, timer: 1, now: 0, whoami: 0, cancel_timer: 1]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  require Fuzzers
  require Logger
  require List
  require Tuple

  defstruct(
    view: %{},              # List of all the nodes in the system and their corresponding health, true if alive, false if dead
    key_value_map: %{},     # key value store internal to each node
    r:  nil,                # Parameter R
    w:  nil,                # Parameter W
    n:  nil,                # Parameter N
    node_list:  nil,        # The list of nodes in clockwise order in the ring
    key_range_data: %{},    # Contains the ranges of keys contained by each node
    gossip_timer: nil,      # Timer to start gossip protocol
    gossip_timeout: nil,    # gossip timeout value
  )

  def new_configuration(
    view,
    key_value_map,
    r,
    w,
    n,
    node_list,
    key_range_data,
    gossip_timeout
  ) do
    %Dynamo{
      view: view,
      key_value_map:  key_value_map,
      r:  r,
      w:  w,
      n:  n,
      node_list:  node_list,
      key_range_data: key_range_data,
      gossip_timeout: gossip_timeout
    }
  end

  ############ UTILITY FUNCTIONS ###################

  # Checks if a node is alive of not
  @spec isNodeAlive(%Dynamo{},atom())  :: boolean()
  def isNodeAlive(state,node) do
    {:ok,{alive,time}} = Map.fetch(state.view,node)
    if alive==true do
      true
    else
      false
    end
  end

  # Get the list of nodes which are alive 
  @spec getAliveNodes(%Dynamo{}) :: [atom()]
  def getAliveNodes(state) do
    aliveNodes = Enum.filter(state.node_list, fn node -> isNodeAlive(state,node)==true end)
  end

  # Check if a given key lies in the node's range
  @spec isKeyInRange({any,any},any) :: boolean
  def isKeyInRange(tuple,key) do
    {min,max} = tuple
    if key>= min and key<=max do
      true
    else
      false
    end
  end

  def isValidRange(keyRange,key) do
    if keyRange==[] do
      false
    end
    [head|tail] = keyRange
    if isKeyInRange(head,key) == true do
      true
    else
      isValidRange(tail,key)
    end
  end

  # Check if a node is a valid owner of the given key
  @spec isValidKeyOwnerNode(%Dynamo{},atom(),non_neg_integer) :: boolean()
  def isValidKeyOwnerNode(state,node,key) do
    {:ok,key_ranges} = Map.fetch(state.key_range_data,node)
    isValidRange(key_ranges,key)
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

  # Put a put request into the state's key_value_map
  @spec put(%Dynamo{},any) :: %Dynamo{}
  def put(state,put_req) do
    {key,value,context} = put_req
    update_key_value_map = Map.put(state.key_value_map,key,{value,context})
    state = Map.put(state,:key_value_map,update_key_value_map)
  end

  def get_first_N_nodes_from_preference_list(key,state) do
    aliveNodes = getAliveNodes(state)       # Get a list of alive nodes
    aliveNodes = aliveNodes ++ aliveNodes   # Extend list by itself to get first N nodes. Extension is done in case some nodes are dead
    key_owners_list = Enum.filter(aliveNodes, fn node -> isValidKeyOwnerNode(state,node,key) == true end)
    [head|tail] = key_owners_list
    key_owners_list = extract_first_n_elements(aliveNodes,head,state.n)
  end
  
  # Return true if the given node exists in the alive nodeList, else return false
  @spec checkIfEligibleNode([atom()],atom()) :: boolean
  def checkIfEligibleNode(nodeList,node) do
    if nodeList == [] do
      false
    end
    [head|tail] = nodeList
    if head==node do
      true
    else
      checkIfEligibleNode(tail,node)
    end
  end

  @spec startNodes(%Dynamo{}) :: %Dynamo{}
  def startNodes(state) do
    temp_view = Enum.map(state.view, fn {k,v} -> {k, {true,Emulation.emu_to_millis(Emulation.now())}} end)
    temp_view = Map.new(temp_view)
    state = Map.put(state,:view,temp_view)
  end

  def startDynamo(state) do
    state = startNodes(state)
    keyRangeList = state.key_range_data[whoami()]
    gossip_timer = Emulation.timer(state.gossip_timeout,:gossip_timer)  # Set gossip timer
    state = Map.put(state,:gossip_timer,gossip_timer) # update state with gossip timer
    listen_client_request(state)                       # Start listening to the clients
  end

  # Check if a put request is valid by checking its context (version info)
  # If the key is not present , we have to write it
  # else if key is already present, check its version/context,
  # If context in the request is latest, then overwrite 
  # else ignore the request ==> return false
  def isValidPutRequest(state,request) do
    {key,value,context} = request
    if state.key_value_map[key]==nil do
      true
    else
      {:ok,{v,c}} = Map.fetch(state.key_value_map,key)
      if c>= context do
        false
      else
        true
      end
    end
  end

  def broadcast_request_to_others(state,message,nodeList) do
    # For all the nodes in the nodeList, send message to all except self
    nodeList
    |> Enum.filter(fn pid -> pid != whoami() end)     # filter self out 
    |> Enum.map(fn pid -> send(pid,message) end)  # send message to remaining nodes
  end

  # Mark the failed node as false in the state's view
  def markFailedNode(view,node) do
    map = Map.put(view,node,{false,Emulation.emu_to_millis(Emulation.now())})
  end

  def get_new_key_range(key_tuple,key_tuple_list) do
    if key_tuple_list == [] do
      [key_tuple]
    end
    [head|tail] = key_tuple_list
    {start_head,end_head} = head
    {start_tuple,end_tuple} = key_tuple
    if start_head == start_tuple and end_head == end_tuple do
      key_tuple_list
    else
      [head] ++ get_new_key_range(key_tuple,tail)
    end
  end

  def get_latest_key_range(state,tup,list) do
    if list == [] do
      state
    else
      [head|tail] = list
      get_new_list = state.key_range_data[head]
      new_key_range_data = get_new_key_range(tup,get_new_list)
      new_map  = Map.put(state.key_range_data,head,new_key_range_data)
      state = %{state | key_range_data: new_map}
      state = get_latest_key_range(state,tup,tail)
    end   
  end

  def get_correct_range(state,tup) do
    {start_range,end_range} = tup
    list = get_first_N_nodes_from_preference_list(start_range,state)
    state = get_latest_key_range(state,tup,list) #Insert the tuple on every list
  end

  def correct_key_range(state,list) do
    if list == [] do
      state
    else
      [head|tail] = list
      state = get_correct_range(state,head)
      state = correct_key_range(state,tail)
    end
  end

  def reconcile_key_range(state,proc_name) do
    list = state.key_range_data[proc_name]
    if list == nil do
      state
    else
      state = correct_key_range(state,list)
    end
  end

  def mark_process_dead(state,proc_name) do
    # mark the proc_name as failed in the state.view
    new_view = markFailedNode(state.view,proc_name)

    # update the state.view with new view
    state = %{state| view: new_view}

    # Reconcile the key ranges
    state = reconcile_key_range(state,proc_name)

    #IO.puts("reconciled Key Range : #{inspect(state)}")
    new_map = Map.put(state.key_range_data,proc_name,[])
    state = %{state| key_range_data: new_map}
  end

  def mark_process_alive(state,keyList) do
    if keyList == [] do
      state
    else
      [head|tail] = keyList
      new_view = Map.put(state.view,head,{true,Emulation.emu_to_millis(Emulation.now())})
      state = %{state|view: new_view}
      state = mark_process_alive(state,tail)
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

  # Get a random process other than self
  def get_random_process(state) do
    true_list = getAliveNodes(state)
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

  def handle_write_request(state,count,request) do
    {sender,key,value,context,keyList} = request
    if count == 0 do
      send(sender,:ok)
      listen_client_request(state)
    else
      receive do
        :gossip_timer -> 
          proc_name = get_random_process(state) 
          message = {:gossip_view}
          send(proc_name,message)
          t = Emulation.timer(state.gossip_timeout,:gossip_timer)
          state = %{state| gossip_timer: t}
          handle_write_request(state,count,request)
        
        {sender,{:gossip_view}} ->   
          message = {:gossip_view,state.view}
          send(sender,message)
          handle_write_request(state,count,request)

        {_,{:gossip_view,other_view}} ->
            Emulation.cancel_timer(state.gossip_timer)
            reconciled_view = reconcile_views(state.view,other_view,state.node_list)
            state = %{state| view: reconciled_view}
            state = reconcile_all_failed_process(state,state.node_list)
            t = Emulation.timer(state.gossip_timeout,:gossip_timer)
            state = %{state| gossip_timer: t}
            handle_write_request(state,count,request)




        {_,:get_state} -> IO.puts("State of #{inspect(whoami())} : #{inspect(state)}")
            handle_write_request(state,count,request)
        {_,{:stop,proc_name}} -> 
          if proc_name == whoami() do
            #IO.puts("#{inspect(whoami())} Dies Now :-(")
          else
              state = mark_process_dead(state,proc_name)
              handle_write_request(state,count,request)
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
              handle_write_request(state,count,request)
              else
                message = %Message.PutResponse{
                  key: key,
                  context: context,
                  success: false
                }
                send(sender,message)
                handle_write_request(state,count,request)
              end
              


      {sender, %Message.PutResponse{
        key: key,
        context: context,
        success: success
      }} -> 
          state = mark_process_alive(state,[sender])
          if success == true do
            handle_write_request(state,count - 1,request)
          else
            handle_write_request(state, count,request)
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
          handle_write_request(state, count,request)
        else 
          {:ok,{val,cont}} = Map.fetch(state.hash_map,key)
          message = %Message.GetResponse{
            key: key,
            value: val,
            context: cont
          }
          send(sender,message)
          handle_write_request(state, count,request)
        end 
              
      

      {sender, %Message.GetResponse{
        key: key,
        value: value,
        context: context
      }} -> 
        state = mark_process_alive(state,[sender])
        handle_write_request(state, count,request)
      
      end
    end
  end

end


