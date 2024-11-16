install Visual C++ Redistributable from https://www.microsoft.com/en-us/download/details.aspx?id=48145

run below in powershell

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r .\requirements.txt
```

restart vs code

```
jupyter nbextension enable --py widgetsnbextension
```