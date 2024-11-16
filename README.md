install Visual C++ Redistributable from https://www.microsoft.com/en-us/download/details.aspx?id=48145

install CUDA toolkit 12.4 (12.4.1?) from https://developer.nvidia.com/cuda-toolkit-archive

install cuDNN for CUDA 12 from https://developer.nvidia.com/cudnn
following https://docs.nvidia.com/deeplearning/cudnn/latest/installation/windows.html#

install scoop from https://scoop.sh/

run below in powershell

```powershell
scoop bucket add versions
scoop install versions/python312
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu124
pip install -r .\requirements.txt
```

restart vs code

```
jupyter nbextension enable --py widgetsnbextension
```