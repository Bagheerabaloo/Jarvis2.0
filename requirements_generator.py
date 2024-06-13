import pkg_resources

# Leggi il file requirements.txt
with open('new_requirements.txt', 'r', encoding="utf-16") as file:
    lines = file.readlines()

# Processa ogni riga
for i, line in enumerate(lines):
    if '@ file' in line:
        # Estrai il nome del pacchetto
        package_name = line.split('@')[0].strip()
        # Ottieni la versione installata del pacchetto
        version = pkg_resources.get_distribution(package_name).version
        # Sostituisci la riga con il nome del pacchetto e la versione
        lines[i] = f"{package_name}=={version}\n"

# Scrivi le righe modificate in un nuovo file
with open('new_requirements.txt', 'w') as file:
    file.writelines(lines)