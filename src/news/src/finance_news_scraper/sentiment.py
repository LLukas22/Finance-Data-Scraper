import logging
import torch 
from torch.nn import functional as f
from transformers import BertForSequenceClassification, BertTokenizer
import numpy as np
import os 

MAX_LENGTH = 512
START_TOKEN = 101
STOP_TOKEN = 102
PADDING_TOKEN = 0
ACTIVE_MASK = 1
IGNORE_MASK = 0
CLASSES = [-1,0,1]
TOKENIZER_MODEL = os.getenv('NEWSSCRAPER_SENTIMENT_TOKENIZER',"ProsusAI/finbert") 
SEQUENZECLASSIFICATION_MODEL = os.getenv('NEWSSCRAPER_SENTIMENT_SEQUENZMODEL',"ProsusAI/finbert") 
MODEL_DIR = os.path.abspath(os.getenv('NEWSSCRAPER_MODEL_DIR',"../../../sentiment_model"))


class  SentimentProvider(object):
    tokenizer: BertTokenizer
    model: torch.jit._script.RecursiveScriptModule
    def __init__(self) -> None:
        self.tokenizer = None
        self.model = None
        os.makedirs(MODEL_DIR,exist_ok=True)

    @property
    def is_model_loaded(self)->bool:
        return self.model is not None and self.tokenizer is not None
    
    def load_model(self)->None:
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.tokenizer = BertTokenizer.from_pretrained(TOKENIZER_MODEL)
        os.makedirs(MODEL_DIR,exist_ok=True)
        if not os.path.isfile(os.path.join(MODEL_DIR,"model.pt")):
            #build the torchscript model to gain some speed
            logging.info("Building torchscript model ...")
            model = BertForSequenceClassification.from_pretrained(SEQUENZECLASSIFICATION_MODEL)
            model.eval()
            input_ids = torch.rand((1, MAX_LENGTH)).long()
            attention_mask = torch.rand((1, MAX_LENGTH)).int()
            self.model = torch.jit.trace(model, [input_ids,attention_mask],strict=False)
            self.model.save(os.path.join(MODEL_DIR,"model.pt"))
            logging.info("Finished torchscript model!")
        else:
            self.model = torch.jit.load(os.path.join(MODEL_DIR,"model.pt"))
            
        self.model.eval()
        self.model = self.model.to(self.device)
        
    def dispose_model(self)->None:
        del self.model
        self.model = None
        del self.tokenizer
        self.tokenizer = None
        
    def get_sentiment(self,text:str) -> tuple[int,np.ndarray]:
        """
        Computes the class and the probability of the logits for the given text.
        """
        if not self.is_model_loaded:
            self.load_model()
            
        with torch.no_grad():
            tokenized = self.encode(text)
            prediction = f.softmax(self.model(tokenized['input_ids'],tokenized['attention_mask'])['logits'], dim=-1).mean(dim=0).cpu().numpy()
            predicted_class = CLASSES[np.argmax(prediction)]
            return predicted_class,prediction
            
    
    def encode(self,text:str):
        """
        Uses the tokenizer to build MAX_LENGTH long slices of the text. 
        """
        with torch.no_grad():
            tokenized = self.tokenizer.encode_plus(text,add_special_tokens=False,return_tensors="pt")
            #to support longer texts we split the sequence and pad it manually => then we pass it to the model 
            split_length = MAX_LENGTH-2
            input_id_chunks = tokenized['input_ids'][0].split(split_length)
            mask_chunks = tokenized['attention_mask'][0].split(split_length)

            padded_ids= []
            padded_masks = []
            for i in range(len(input_id_chunks)):
                padded_ids.append(torch.cat([torch.Tensor([START_TOKEN]),input_id_chunks[i],torch.Tensor([STOP_TOKEN])]))
                padded_masks.append(torch.cat([torch.Tensor([ACTIVE_MASK]),mask_chunks[i],torch.Tensor([ACTIVE_MASK])]))
                
            for i in range(len(padded_ids)):
                padding_length = MAX_LENGTH - len(padded_ids[i])
                if padding_length > 0:
                    padded_ids[i] = torch.cat([padded_ids[i],torch.Tensor([PADDING_TOKEN]*padding_length)])
                    padded_masks[i] = torch.cat([padded_masks[i],torch.Tensor([IGNORE_MASK]*padding_length)])

            input_ids = torch.stack(padded_ids).long().to(self.device)
            attention_mask = torch.stack(padded_masks).int().to(self.device)
            return{
                'input_ids': input_ids,
                'attention_mask': attention_mask
            }
            
            
if __name__ == "__main__":
    sentimentProvider = SentimentProvider()
    sentimentProvider.load_model()
    sentimentProvider.get_sentiment("Hello World "*1000)
 