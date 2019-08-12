
# coding: utf-8

# # Hadoop Streaming assignment 1: Words Rating

# The purpose of this task is to create your own WordCount program for Wikipedia dump processing and learn basic concepts of the MapReduce.
# 
# In this task you have to find the 7th word by popularity and its quantity in the reverse order (most popular first) in Wikipedia data (`/data/wiki/en_articles_part`).
# 
# There are several points for this task:
# 
# 1) As an output, you have to get the 7th word and its quantity separated by a tab character.
# 
# 2) You must use the second job to obtain a totally ordered result.
# 
# 3) Do not forget to redirect all trash and output to /dev/null.
# 
# Here you can find the draft of the task main steps. You can use other methods for solution obtaining.

# ## Step 1. Create mapper and reducer.
# 
# <b>Hint:</b>  Demo task contains almost all the necessary pieces to complete this assignment. You may use the demo to implement the first MapReduce Job.

# In[1]:


get_ipython().run_cell_magic('writefile', 'mapper1.py', '\n# Your code for mapper here.\nimport sys\nimport re\n\nreload(sys)\nsys.setdefaultencoding(\'utf-8\') # required to convert to unicode\n\nfor line in sys.stdin:\n    try:\n        article_id, text = unicode(line.strip()).split(\'\\t\', 1)\n    except ValueError as e:\n        continue\n    words = re.split("\\W*\\s+\\W*", text, flags=re.UNICODE)\n    for word in words:\n        print >> sys.stderr, "reporter:counter:Wiki stats,Total words,%d" % 1\n        print "%s\\t%d" % (word.lower(), 1)')


# In[9]:


get_ipython().run_cell_magic('writefile', 'reducer1.py', "\n# Your code for reducer here.\nimport sys\n\ncurrent_word = None\ncurrent_count = 0\nword = None\n\nfor line in sys.stdin:\n    try:\n        word, count = line.strip().split('\\t', 1)\n        count = int(count)\n        if current_word == word:\n            current_count += count\n        else:\n            if current_word:\n                print '%s\\t%s' % (current_word, current_count)\n            current_count = count\n            current_word = word            \n    except ValueError as e:\n        continue\n\nif current_word == word:\n    print '%s\\t%s' % (current_word, current_count)       ")


# In[3]:


# You can use this cell for other experiments: for example, for combiner.


# ## Step 2. Create sort job.
# 
# <b>Hint:</b> You may use MapReduce comparator to solve this step. Make sure that the keys are sorted in ascending order.

# In[19]:


get_ipython().run_cell_magic('writefile', 'mapper2.py', "\nimport sys\n\ncurrent_word = None\ncurrent_count = 0\nword = None\n\nfor line in sys.stdin:\n    try:\n        word, count = line.strip().split('\\t', 1)\n        count = int(count)\n        print '%s\\t%s' % (current_word, current_count)\n         \n    except ValueError as e:\n        continue")


# In[27]:


get_ipython().run_cell_magic('writefile', 'reducer2.py', "\nimport sys\n\ncurrent_word = None\ncurrent_count = 0\nword = None\n\nfor line in sys.stdin:\n    try:\n        word, count = line.strip().split('\\t', 1)\n        count = int(count)\n        print '%s\\t%s' % (current_word, current_count)\n         \n    except ValueError as e:\n        continue")


# ## Step 3. Bash commands
# 
# <b> Hint: </b> For printing the exact row you may use basic UNIX commands. For instance, sed/head/tail/... (if you know other commands, you can use them).
# 
# To run both jobs, you must use two consecutive yarn-commands. Remember that the input for the second job is the ouput for the first job.

# In[ ]:


get_ipython().run_cell_magic('bash', '', '\nOUT_DIR="assignment1_"$(date +"%s%6N")\n\n# Code for your first job\nNUM_REDUCERS=8\n# yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar ...\nyarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \\\n    -D mapred.jab.name="Streaming wordCount" \\\n    -D mapreduce.job.reduces=${NUM_REDUCERS} \\\n    -files mapper1.py,reducer1.py \\\n    -mapper "python mapper1.py" \\\n    -combiner "python reducer1.py" \\\n    -reducer "python reducer1.py" \\\n    -input /data/wiki/en_articles_part \\\n    -output ${OUT_DIR} > /dev/null\n    \n# Code for your second job\n# yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar ...\nNUM_REDUCERS_2=1\nOUT_DIR_2="assignment1_2"$(date +"%s%6N")\n# yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar ...\nyarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \\\n    -D mapred.jab.name="Streaming wordCount 2" \\\n    -D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator \\\n    -D map.output.key.field.separator="\\t" \\\n    -D mapreduce.partition.keycomparator.options="-k1,2nr" \\\n    -D mapreduce.job.reduces=${NUM_REDUCERS_2} \\\n    -files mapper2.py,reducer2.py \\\n    -mapper "python mapper2.py" \\\n    -reducer "python reducer2.py" \\\n    -input ${OUT_DIR} \\\n    -output ${OUT_DIR_2} > /dev/null\n# Code for obtaining the results\nhdfs dfs -cat ${OUT_DIR_2}/part-00000 | sed -n \'7p;8q\'\nhdfs dfs -rm -r -skipTrash ${OUT_DIR}* > /dev/null\nhdfs dfs -rm -r -skipTrash ${OUT_DIR_2}* > /dev/null')


# In[24]:


get_ipython().run_cell_magic('bash', '', 'OUT_DIR="assignment1_"$(date +"%s%6N")\nhdfs dfs -cat ${OUT_DIR}_2/part-00000 | sed -n \'7p;8q\'')


# In[29]:


get_ipython().run_cell_magic('bash', '', 'hdfs dfs -rm -r -skipTrash ${OUT_DIR}* > /dev/null\nhdfs dfs -rm -r -skipTrash ${OUT_DIR_2}* > /dev/null')

