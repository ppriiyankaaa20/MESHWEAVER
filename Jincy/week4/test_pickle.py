import cloudpickle


def add(a, b):
    return a + b


# Function serialize 
data = cloudpickle.dumps(add)

print("Function serialized successfully!")
print("Serialized data size:", len(data), "bytes")


# Function deserialize 
received_function = cloudpickle.loads(data)

# Function execute 
result = received_function(10, 20)

print("Function executed successfully!")
print("Result:", result)