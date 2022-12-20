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
#Write : No of Writes Required = 1 (For this example) Highly available System
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

@spec checkProcessAlive(%Dynamo{},atom()) :: boolean()
def checkProcessAlive(state,procName) do
  {:ok, {ans, time}} = Map.fetch(state.view,procName)
  if ans == true do
    true
  else 
    false #Always remember will have to change it
  end
end


#Returns the List of all the Nodes whoose view is set as true.
@spec fetchAliveList(%Dynamo{}) :: [atom()]
def fetchAliveList(state) do
  # READ: state.view = [[true, 12000], [true, 22000], [true, 30000]]
  # This will check if first item in element == true and add it to view
  list = Enum.filter(state.node_list, fn proc -> checkProcessAlive(state,proc) == true end)
  #IO.puts(" List: #{inspect(list)}")
  list
end

@spec isValidRange({any, any}, any) :: boolean
def isValidRange(tup, key) do
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
    if isValidRange(head,key) == true do
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

def getElements(list,n) do
  if n==0 do
    []
  else
    [head|tail] = list
    ans = [head] ++ getElements(tail, n - 1)
  end
end

def getFirstNElements(list,element, n) do
  if list==[] do
    []
  else 
    [head | tail] = list
    if head == element do
      ans = getElements(list,n)
    else 
      getFirstNElements(tail,element,n)
    end
  end
end

def write_to_state(state,request) do
  {key,value,context} = request
  new_hash_map = Map.put(state.hash_map,key,{value,context})
  state = %{state|hash_map: new_hash_map}
  state
end

@spec getKeyList(non_neg_integer(), %Dynamo{}) :: [atom()]
def getKeyList(key, state) do
  list = fetchAliveList(state)
  list = list ++ list
  key_list = Enum.filter(list, fn proc -> is_valid_proc(state,proc,key) == true end)
  [head|tail] = key_list
  key_list = getFirstNElements(list,head,state.n_param)
end

@spec isEligible([atom()],atom()) :: boolean()
def isEligible(list, proc_name) do
  if list == [] do
    false
  else 
    [head|tail] = list
    if head==proc_name do
      true
    else
      isEligible(tail,proc_name)
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

@spec start_Dynamo(%Dynamo{})::no_return()
def start_Dynamo(state) do
  #Time to start Dynamo.
  #For the First Time Will Listen to Get/Put Request
  state = set_all_node_working(state)
  list_of_key_ranges = state.key_range_data[whoami()]
  t = Emulation.timer(state.gossip_timeout,:gossip_timer)
  state = %{state| gossip_timer: t}
  listen_requests(state)
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
    [tup]
  else
    [head|tail] = list
    {start_head,end_head} = head
    {start_tup,end_tup} = tup
    if start_head==start_tup and end_head==end_tup do
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

# tup : {0,99}
def get_correct_range(state,tup) do
  {start_range,end_range} = tup
  list = getKeyList(start_range,state)
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
  true_list = fetchAliveList(state)
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

def switch_to_write_mode(state,count,request) do
  {sender,key,value,context,keyList} = request
  if count == 0 do
    send(sender,:ok)
    listen_requests(state)
  else
    #Hmm Listen to Requests
    #Dont take any more Client Requests
    #However take requests from the Other Nodes
    receive do
      :gossip_timer -> 
        proc_name = get_random_process(state) 
        message = {:gossip_protocol}
        send(proc_name,message)
        t = Emulation.timer(state.gossip_timeout,:gossip_timer)
        state = %{state| gossip_timer: t}
        switch_to_write_mode(state,count,request)
      
      {sender,{:gossip_protocol}} ->   
        message = {:gossip_protocol,state.view}
        send(sender,message)
        switch_to_write_mode(state,count,request)

      {_,{:gossip_protocol,other_view}} ->
        Emulation.cancel_timer(state.gossip_timer)
        reconciled_view = reconcile_views(state.view,other_view,state.node_list)
        state = %{state| view: reconciled_view}
        state = reconcile_all_failed_process(state,state.node_list)
        t = Emulation.timer(state.gossip_timeout,:gossip_timer)
        state = %{state| gossip_timer: t}
        switch_to_write_mode(state,count,request)




      {_,:get_state} -> IO.puts("State of #{inspect(whoami())} : #{inspect(state)}")
        switch_to_write_mode(state,count,request)
      {_,{:stop_node,proc_name}} -> 
        if proc_name == whoami() do
          #IO.puts("#{inspect(whoami())} Dies Now :-(")
        else
            state = mark_process_dead(state,proc_name)
            switch_to_write_mode(state,count,request)
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
          switch_to_write_mode(state,count,request)
        else
          message = %Message.PutResponse{
            key: key,
            context: context,
            success: false
          }
          send(sender,message)
          switch_to_write_mode(state,count,request)
        end
            


    {sender, %Message.PutResponse{
      key: key,
      context: context,
      success: success
    }} -> 
      # IO.puts("[dynamo] put response received from #{sender} with success #{success}")
        state = mark_process_alive(state,[sender])
        if success == true do
          switch_to_write_mode(state,count - 1,request)
        else
          switch_to_write_mode(state,count,request)
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
        switch_to_write_mode(state, count,request)
      else 
        {:ok,{val,cont}} = Map.fetch(state.hash_map,key)
        message = %Message.GetResponse{
          key: key,
          value: val,
          context: cont
        }
        send(sender,message)
        switch_to_write_mode(state, count,request)
      end 
            
    

    {sender, %Message.GetResponse{
      key: key,
      value: value,
      context: context
    }} -> 
      state = mark_process_alive(state,[sender])
      switch_to_write_mode(state, count,request)
    
    end
  end
end

def handle_write_request(state,request) do
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
    switch_to_write_mode(state,state.write_param - 1,request)
  else
    listen_requests(state)
  end
end

def switch_to_read_mode(state,request,response,read_count) do
  if read_count == 0 do
    {sender,key,keyList} = request
    send(sender,response)
    listen_requests(state)
  else
    receive do
      :gossip_timer -> 
        proc_name = get_random_process(state) 
        message = {:gossip_protocol}
        send(proc_name,message)
        t = Emulation.timer(state.gossip_timeout,:gossip_timer)
        state = %{state| gossip_timer: t}
        switch_to_read_mode(state,request,response,read_count)
      
      {sender,{:gossip_protocol}} ->   
        message = {:gossip_protocol,state.view}
        send(sender,message)
        switch_to_read_mode(state,request,response,read_count)

      {_,{:gossip_protocol,other_view}} ->
        Emulation.cancel_timer(state.gossip_timer)
        reconciled_view = reconcile_views(state.view,other_view,state.node_list)
        state = %{state| view: reconciled_view}
        state = reconcile_all_failed_process(state,state.node_list)
        t = Emulation.timer(state.gossip_timeout,:gossip_timer)
        state = %{state| gossip_timer: t}
        switch_to_read_mode(state,request,response,read_count)

      {_,:get_state} -> IO.puts("State of #{inspect(whoami())} : #{inspect(state)}")
          switch_to_read_mode(state,request,response,read_count)

      {_,{:stop_node,proc_name}} -> 
        if proc_name == whoami() do
          #IO.puts("#{inspect(whoami())} Dies Now :-(")
        else
            state = mark_process_dead(state,proc_name)
            switch_to_read_mode(state,request,response,read_count)
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
          switch_to_read_mode(state,request,response,read_count)
        else
          message = %Message.PutResponse{
            key: key,
            context: context,
            success: false
          }
          send(sender,message)
          switch_to_read_mode(state,request,response,read_count)
        end
      
      {sender, %Message.PutResponse{
        key: key,
        context: context
      }} -> 
        state = mark_process_alive(state,[sender])  
        switch_to_read_mode(state,request,response,read_count)

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
        switch_to_read_mode(state,request,response,read_count)
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
        switch_to_read_mode(state,request,response,read_count)
      end 

      {sender, %Message.GetResponse{
        key: key,
        value: value,
        context: context
        }} -> 
          state = mark_process_alive(state,[sender])
          if value == nil do
            switch_to_read_mode(state,request,response,read_count - 1)
          else
            {response_value,response_context} = response
            if response_context < context do
              response = {value,context}
              switch_to_read_mode(state,request,response,read_count - 1)
            else
              switch_to_read_mode(state,request,response,read_count - 1)
            end

          end
    end
  end 
end

def handle_read_request(state,request) do
  {sender,key,keyList} = request
  message = %Message.GetRequest{
    key: key
  }
  broadcast_request_to_others(state,message,keyList)
  if state.hash_map[key] == nil do
    response = {nil,nil}
    switch_to_read_mode(state,request,response,state.read_param - 1)
  else
    {:ok,response} = Map.fetch(state.hash_map,key)
    switch_to_read_mode(state,request,response,state.read_param - 1)
  end

end

@spec listen_requests(%Dynamo{})::no_return()
def listen_requests(state) do
  receive do
    :gossip_timer -> 
      proc_name = get_random_process(state) 
      message = {:gossip_protocol}
      send(proc_name,message)
      t = Emulation.timer(state.gossip_timeout,:gossip_timer)
      state = %{state| gossip_timer: t}
      listen_requests(state)
                      
      
      {sender,{:gossip_protocol}} ->   
          message = {:gossip_protocol,state.view}
          send(sender,message)
          listen_requests(state)

      {_,{:gossip_protocol,other_view}} ->
          Emulation.cancel_timer(state.gossip_timer)
          reconciled_view = reconcile_views(state.view,other_view,state.node_list)
          state = %{state| view: reconciled_view}
          state = reconcile_all_failed_process(state,state.node_list)
          t = Emulation.timer(state.gossip_timeout,:gossip_timer)
          state = %{state| gossip_timer: t}
          listen_requests(state)

        
    {_,:get_state} -> IO.puts("State of #{inspect(whoami())} : #{inspect(state)}")
        listen_requests(state)

    {_,{:stop_node,proc_name}} -> 
    if proc_name == whoami() do
    else
      state = mark_process_dead(state,proc_name)  
      listen_requests(state)
    end

    {sender, {:put, key, value, context}} ->
      IO.puts("PUT request received at #{whoami()} for key:#{key}, value:#{value}, context:#{context}")
      keyList = getKeyList(key,state)
      node_eligible = isEligible(keyList,whoami())
      if node_eligible == true do
        request = {sender,key,value,context,keyList}
        handle_write_request(state,request)
      else
        
        [head|tail] = keyList
        message = {:redirect,head}
        send(sender,message)
        listen_requests(state)
      end


    {sender, {:get, key}} ->
      keyList = getKeyList(key,state)
      node_eligible = isEligible(keyList,whoami())
      if node_eligible == true do
        request = {sender,key,keyList}
        handle_read_request(state,request)
      else
        [head|tail] = keyList
        message = {:redirect,head}
        send(sender,message)
        listen_requests(state)
      end


    #Time For Response from Other Nodes :
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
        listen_requests(state)
      else
        message = %Message.PutResponse{
          key: key,
          context: context,
          success: false
        }
        send(sender,message)
        listen_requests(state)
      end
      

     {sender, %Message.PutResponse{
      key: key,
      context: context
     }} -> 
      state = mark_process_alive(state,[sender])  
      listen_requests(state)

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
        listen_requests(state)
      else 
        {:ok,{val,cont}} = Map.fetch(state.hash_map,key)
        message = %Message.GetResponse{
          key: key,
          value: val,
          context: cont
        }
        send(sender,message)
        listen_requests(state)
      end 
      

     {sender, %Message.GetResponse{
      key: key,
      value: value,
      context: context
     }} -> 
      state = mark_process_alive(state,[sender])
      listen_requests(state)

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
    contact_node = client.node
    message = {:put, key, value, context}
    send(contact_node, message)
    receive do
      {_, {:redirect, contact_node}} ->
        put(%{client | node: contact_node}, key, value, context)

      {_, :ok} ->
        {:ok, client}

      m -> IO.puts("error message #{inspect{m}}")

    end
  end

  @spec get(%Client{}, atom()) :: {:ok, %Client{}}
  def get(client, key) do
    contact_node = client.node
    message = {:get, key}
    send(contact_node, message)
    receive do
      {_, {:redirect, contact_node}} ->
          get(%{client| node: contact_node}, key)

      {_, {value, staleness}} -> {{value, staleness}, client}


    end
  end

  @spec updateNode(%Client{}, atom()) :: {:ok, atom()}
  def updateNode(client, new_node) do
    client = %{client|node: new_node}
    client
  end

  @spec stopProcess(%Client{},atom()) :: :no_return
  def stopProcess(client,proc_name) do
    contact_node = client.node
    message = {:stop_node,proc_name}
    IO.puts("Name of Failed  Process : #{inspect(message)}")
    send(proc_name,message)
    send(contact_node,message)
  end


end