from service.init.confluent import *

if __name__ == "__main__":
    # Load schemas (in prod: read from files or Git)
    with open("infra/confluent/schemas/reviews.avsc", "r") as f:
        reviews_schema = f.read()
    
    # Set compatibility first
    set_compatibility("review")
    
    # Register
    register_schema("review", reviews_schema)