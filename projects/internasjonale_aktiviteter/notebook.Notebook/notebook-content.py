# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "0c35c7eb-51e7-a8d4-469c-bc6b9a9fa1ca",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

# Use the 2 magic commands below to reload the modules if your module has updates during the current session. You only need to run the commands once.
%load_ext autoreload
%autoreload 2
import os
import sys
#if 'env.src_python.test_function_package' in sys.modules:
#    del sys.modules['env.src_python.test_function_package']

import env.src_python.test_function_package as test_function_package

# Now import it again

# Try using the function
sentence_a = "this is a string"
sentence_b = test_function_package.foo_bar()
print(f"{sentence_a} {sentence_b}")

#file_path = test_function_package.__file__
#print(f"File path: {file_path}")

# Read and print the file contents
#with open(file_path, 'r') as f:
#    print(f.read())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


setnence_a = "this is a string"
sentence_b = test_function_package.foo_bar()
print(f"{sentence_a} {sentence_b}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
