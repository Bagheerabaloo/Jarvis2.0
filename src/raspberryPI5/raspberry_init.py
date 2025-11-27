import platform
import os
from dotenv import load_dotenv

def running_on_raspberry() -> bool:
    """
    Ritorna True se stiamo girando su Raspberry Pi (Linux),
    False altrimenti.

    Usa prima una variabile d'ambiente RUN_ENV (se presente),
    altrimenti auto-detect.
    """
    # 1) Override manuale via env (più forte di tutto)
    run_env = os.getenv("RUN_ENV")
    if run_env == "raspberry":
        return True
    if run_env == "pc":
        return False

    # 2) Auto-detect: se non siamo su Linux, sicuramente non è Raspberry
    if platform.system() != "Linux":
        return False

    # 3) Su Linux, controlliamo il model del device
    try:
        with open("/sys/firmware/devicetree/base/model", "r") as f:
            model = f.read()
        return "Raspberry Pi" in model
    except FileNotFoundError:
        return False

load_dotenv()

IS_RASPBERRY = running_on_raspberry()
print("Running on Raspberry:", IS_RASPBERRY)