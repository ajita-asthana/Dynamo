defmodule Message.GetRequest do
    alias __MODULE__
    defstruct(
        key: nil
    )
    @spec new(non_neg_integer()) :: %GetRequest{
        key: non_neg_integer()
    }
    def new(key) do
        %GetRequest{
            key: key
        }
    end
end

defmodule Message.GetResponse do
    alias __MODULE__
    defstruct(
        key: nil,
        value: nil,
        context: nil
    )
    @spec new(non_neg_integer(), non_neg_integer(), non_neg_integer()) :: %GetResponse{
        key: non_neg_integer(),
        value: non_neg_integer(),
        context: non_neg_integer()
    }
    def new(key,value,context) do
        %GetResponse{
            key: key,
            value: value,
            context: context
        }
    end
end

defmodule Message.PutRequest do
    alias __MODULE__
    defstruct(
        key: nil,
        value: nil,
        context: nil
    )
    @spec new(non_neg_integer(), non_neg_integer(), non_neg_integer()) :: %PutRequest{
        key: non_neg_integer(),
        value: non_neg_integer(),
        context: non_neg_integer()
    }
    def new(key,value,context) do
        %PutRequest{
            key: key,
            value: value,
            context: context
        }
    end
end

defmodule Message.PutResponse do
    alias __MODULE__
    defstruct(
        key: nil,
        context: nil,
        success: nil
    )
    @spec new(non_neg_integer(),non_neg_integer(),boolean()) :: %PutResponse{
        key: non_neg_integer(),
        context: non_neg_integer(),
        success: boolean()
    }
    def new(key,context,succ) do
        %PutResponse{
            key: key,
            context: context,
            success: succ
        }
    end
end
