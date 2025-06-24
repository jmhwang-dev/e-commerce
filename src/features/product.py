from utils import get_dataset, BronzeDataName

if __name__=="__main__":
    products = get_dataset(BronzeDataName.PRODUCTS)
    category_eng = get_dataset(BronzeDataName.CATEGORY)