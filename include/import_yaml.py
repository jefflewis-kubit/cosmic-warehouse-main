import yaml

with open('config.yaml','r') as file:
    generator_config = yaml.safe_load(file)

#print(generator_config)

print(generator_config.get('days_since_start_thresholds').keys())

