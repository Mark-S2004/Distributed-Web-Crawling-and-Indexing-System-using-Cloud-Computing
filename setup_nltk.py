import nltk
import ssl

try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context

# Download required NLTK data
print("Downloading required NLTK data...")

# Create a list of all required NLTK packages
required_packages = [
    'punkt',
    'punkt_tab',
    'wordnet',
    'stopwords',
    'averaged_perceptron_tagger',
    'maxent_ne_chunker',
    'words'
]

# Download each package
for package in required_packages:
    print(f"Downloading {package}...")
    try:
        nltk.download(package, quiet=True)
        print(f"Successfully downloaded {package}")
    except Exception as e:
        print(f"Error downloading {package}: {str(e)}")

print("\nNLTK setup complete!")

# Verify the downloads
print("\nVerifying downloads...")
try:
    from nltk.tokenize import word_tokenize, sent_tokenize
    test_text = "This is a test sentence. This is another test sentence."
    tokens = word_tokenize(test_text)
    sentences = sent_tokenize(test_text)
    print("Tokenization test successful!")
except Exception as e:
    print(f"Verification failed: {str(e)}") 