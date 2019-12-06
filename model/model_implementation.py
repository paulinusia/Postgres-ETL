#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import re
import string
import numpy as np

from sklearn.model_selection import train_test_split
from string import digits
import matplotlib.pyplot as plt
#get_ipython().run_line_magic('matplotlib', 'inline')
from sklearn.utils import shuffle
from sklearn.model_selection import train_test_split
import keras
from keras.layers import Input, LSTM, Embedding, Dense
from keras.models import Model
import pickle
import os
os.environ['KMP_DUPLICATE_LIB_OK'] = 'True'
CUDA_VISIBLE_DEVICES=1
os.environ["CUDA_VISIBLE_DEVICES"]="0,1"


# In[2]:


txt = pd.read_csv('../output_files/master.csv', index_col=False, error_bad_lines=False)


# In[3]:


type(txt)
txt = txt.applymap(str)
all_comment_words = set()
all_reply_words = set()
length_list_comment = []
length_list_reply = []
txt.shape


# In[4]:


#lower
txt.reply = txt.reply.apply(lambda x: x.lower())
txt.comment = txt.comment.apply(lambda x: x.lower())

#quote removal
txt.comment = txt.comment.apply(lambda x: re.sub("''", '', x))
txt.reply = txt.reply.apply(lambda x: re.sub("''", '', x))
#special char
txt.comment = txt.comment.apply(lambda x: re.sub("[|\^&+\-%*/=!>]", '', x))
txt.reply = txt.reply.apply(lambda x: re.sub("[|\^&+\-%*/=!>]", '', x))

#punctuation
txt.comment = txt.comment.apply(lambda x: re.sub(r'[^\w\s]', "", x))
txt.reply = txt.reply.apply(lambda x: re.sub(r'[^\w\s]', "", x))

#numbers
txt.comment = txt.comment.apply(lambda x: re.sub("[0-9]", '', x))
txt.reply = txt.reply.apply(lambda x: re.sub("[0-9]", '', x))

#extra spacing
#txt.comment = txt.comment.apply("[/^\s+|\s+$|\s+(?=\s)/g]", "")
#txt.reply = txt.reply.replace("[/^\s+|\s+$|\s+(?=\s)/g]", "")

#Start and end tokens ?

txt.reply = txt.reply.apply(lambda x : 'START_' + x + '_END')


# In[5]:


txt


# In[6]:


#create list of unique words for both commetn and reply
for comment in txt.comment:
    for word in comment.split():
        if word not in all_comment_words:
            all_comment_words.add(word)
            
for reply in txt.reply:
    for word in reply.split():
        if word not in all_reply_words:
            all_reply_words.add(word)


# In[7]:


#finds the max comment length
for l in txt.comment:
    length_list_comment.append(len(l.split(' ')))
    
max_length_src = np.max(length_list_comment) 

max_length_src


# In[8]:


#finds the maximum reply length
for l in txt.reply:
    length_list_reply.append(len(l.split(' ')))
    
max_length_tar = np.max(length_list_reply)
max_length_tar
    


# In[9]:


#Sort dictionaries alphabetically
source_words = sorted(list(all_comment_words))
target_words = sorted(list(all_reply_words))

print(source_words)
# In[10]:


# Vocab sizes
#increase num_encoder_token by 1, that is num_encoder_token -. vocab_size + 1. 

num_encoder_tokens = len(all_comment_words)+1
num_decoder_tokens = len(all_reply_words)
num_decoder_tokens += 1 #padding
num_encoder_tokens, num_decoder_tokens


# In[11]:


#WORD TO TOKEN DICTIONARY (each word in dict gets an index)

source_token_index = dict([(word, i+1) for i, word in enumerate(source_words)])
target_token_index = dict([(word, i+1) for i, word in enumerate(target_words)])
source_token_index


# In[12]:


# Create token to word dictionary for both source and target
#puts index first
reverse_source_char_index = dict((i,word) for word, i in source_token_index.items())
reverse_target_char_index = dict((i,word) for word, i in target_token_index.items())
reverse_source_char_index


# In[13]:


# Train - Test Split #randomized
X, y = txt.comment, txt.reply
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.1)

#x_train = comment , y_train = reply


# In[14]:


#Save the train and test dataframes for reproducing the results later, as they are shuffled.
X_train.to_pickle('X_train.pkl')
X_test.to_pickle('X_test.pkl')


# In[15]:


#batch_size = number of examples pulled at once for the model
def generate_batch(X = X_train, y = y_train, batch_size = 1000):
    ''' Generate a batch of data '''
    while True:
        #0=min, len(X)= max, batch_size=the step inbetween values
        for j in range(0, len(X), batch_size):
            #matrix of zeros 128*47
            encoder_source_data = np.zeros((batch_size, max_length_src),dtype='float32')
            #matrix of zeros 128*38
            decoder_source_data = np.zeros((batch_size, max_length_tar),dtype='float32')
            #matrix of zeros 128*38*28427
            decoder_target_data = np.zeros((batch_size, max_length_tar, num_decoder_tokens),dtype='float32')
            
            #iterates over source_text and target_text together
            #zips(iterates through) (current batch from X, and the current batch from y)
            for i, (source_text, target_text) in enumerate(zip(X[j:j+batch_size], y[j:j+batch_size])):
                #Enumerate() method adds a counter to an iterable and returns it in a form of enumerate object.
                #iterates over t? value and word in the in the comments AKA source_text
                for t, word in enumerate(source_text.split()):
                    #added to encoder
                    encoder_source_data[i, t] = source_token_index[word] # encoder input seq
                    
                for t, word in enumerate(target_text.split()):
                    if t<len(target_text.split())-1:
                        decoder_source_data[i, t] = target_token_index[word] # decoder input seq
                    if t>0:
                        # decoder target sequence (one hot encoded)
                        # does not include the START_ token
                        # Offset by one timestep
                        decoder_target_data[i, t - 1, target_token_index[word]] = 1.
            yield([encoder_source_data, decoder_source_data], decoder_target_data)


# In[16]:


#Encoder Decoder Model Architure
#variables are variables which we do not directly observe, but which we assume to exist 

latent_dim = 50


# In[17]:


#Encoder
encoder_inputs = Input(shape=(None,))
enc_emb =  Embedding(num_encoder_tokens, latent_dim, mask_zero = True)(encoder_inputs)
encoder_lstm = LSTM(latent_dim, return_state=True)
encoder_outputs, state_h, state_c = encoder_lstm(enc_emb)
# We discard `encoder_outputs` and only keep the states.
encoder_states = [state_h, state_c]


# In[18]:


# Set up the decoder, using `encoder_states` as initial state.
decoder_inputs = Input(shape=(None,))
dec_emb_layer = Embedding(num_decoder_tokens, latent_dim, mask_zero = True)
dec_emb = dec_emb_layer(decoder_inputs)
# We set up our decoder to return full output sequences,
# and to return internal states as well. We don't use the
# return states in the training model, but we will use them in inference.
decoder_lstm = LSTM(latent_dim, return_sequences=True, return_state=True)
decoder_outputs, _, _ = decoder_lstm(dec_emb,
                                     initial_state=encoder_states)
decoder_dense = Dense(num_decoder_tokens, activation='softmax')
decoder_outputs = decoder_dense(decoder_outputs)

# Define the model that will turn
# `encoder_input_data` & `decoder_input_data` into `decoder_target_data`
model = Model([encoder_inputs, decoder_inputs], decoder_outputs)


# In[19]:


model.compile(optimizer='rmsprop', loss='categorical_crossentropy', metrics=['acc'])


# In[20]:


train_samples = len(X_train)
val_samples = len(X_test)
batch_size = 100
epochs = 50

train_samples, val_samples


# In[ ]:


model.fit_generator(generator = generate_batch(X_train, y_train, batch_size = batch_size),
                    steps_per_epoch = train_samples//batch_size,
                    epochs=epochs,
                    validation_data = generate_batch(X_test, y_test, batch_size = batch_size),
                    validation_steps = val_samples//batch_size)


# In[ ]:


model.save_weights('nmt_weights.h5')
model.load_weights('nmt_weights.h5')


# In[ ]:


#INFERENCE SET UP
# Encode the input sequence to get the "thought vectors"
encoder_model = Model(encoder_inputs, encoder_states)

# Decoder setup
# Below tensors will hold the states of the previous time step
decoder_state_input_h = Input(shape=(latent_dim,))
decoder_state_input_c = Input(shape=(latent_dim,))
decoder_states_inputs = [decoder_state_input_h, decoder_state_input_c]

dec_emb2= dec_emb_layer(decoder_inputs) # Get the embeddings of the decoder sequence

# To predict the next word in the sequence, set the initial states to the states from the previous time step
decoder_outputs2, state_h2, state_c2 = decoder_lstm(dec_emb2, initial_state=decoder_states_inputs)
decoder_states2 = [state_h2, state_c2]
decoder_outputs2 = decoder_dense(decoder_outputs2) # A dense softmax layer to generate prob dist. over the target vocabulary

# Final decoder model
decoder_model = Model(
    [decoder_inputs] + decoder_states_inputs,
    [decoder_outputs2] + decoder_states2)


# In[ ]:


def decode_sequence(input_seq):
    # Encode the input as state vectors.
    states_value = encoder_model.predict(input_seq)
    # Generate empty target sequence of length 1.
    target_seq = np.zeros((1,1))
    # Populate the first character of target sequence with the start character.
    target_seq[0, 0] = target_token_index['START_']

    # Sampling loop for a batch of sequences
    # (to simplify, here we assume a batch of size 1).
    stop_condition = False
    decoded_sentence = ''
    while not stop_condition:
        output_tokens, h, c = decoder_model.predict([target_seq] + states_value)

        # Sample a token
        sampled_token_index = np.argmax(output_tokens[0, -1, :])
        sampled_char = reverse_target_char_index[sampled_token_index]
        decoded_sentence += ' '+sampled_char

        # Exit condition: either hit max length
        # or find stop character.
        if (sampled_char == '_END' or
           len(decoded_sentence) > 50):
            stop_condition = True

        # Update the target sequence (of length 1).
        target_seq = np.zeros((1,1))
        target_seq[0, 0] = sampled_token_index

        # Update states
        states_value = [h, c]

    return decoded_sentence


# In[ ]:


train_gen = generate_batch(X_train, y_train, batch_size = 1)
k=-1


# In[ ]:


k+=1
(input_seq, actual_output), _ = next(train_gen)
decoded_sentence = decode_sequence(input_seq)
print('Input:', X_train[k:k+1].values[0])
print('Actual reply:', y_train[k:k+1].values[0][6:-4])
print('Predicted reply:', decoded_sentence[:-4])


# In[ ]:


k+=1
(input_seq, actual_output), _ = next(train_gen)
decoded_sentence = decode_sequence(input_seq)
print('Input:', X_train[k:k+1].values[0])
print('Actual:', y_train[k:k+1].values[0][6:-4])
print('Predicted:', decoded_sentence[:-4])


# In[ ]:


#How to take user input??
k+=1
(input_seq, actual_output), _ = next(train_gen)
decoded_sentence = decode_sequence(input_seq)

print('Input English sentence:', 'My name is Paulina')
print('Actual:', 'Thats cool! My name is Todd')
print('Predicted:', decoded_sentence[:-4])


# In[ ]:





# In[ ]:




