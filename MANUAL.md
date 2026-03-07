# EPUB Translate Manual

Narzędzie: `tools/epub_translate.py`

Służy do bezpiecznego tłumaczenia EPUBów z zachowaniem struktury XHTML/HTML, postępu pracy i składania nowego pliku EPUB.

## Wymagania

- Python 3.11+
- ustawiony `OPENAI_API_KEY`
- książka źródłowa w formacie `.epub`

PowerShell:

```powershell
$env:OPENAI_API_KEY="twoj_klucz"
```

CMD:

```cmd
set OPENAI_API_KEY=twoj_klucz
```

## Konwencja projektów

Każda książka ma osobny projekt w `projects/`.

Zalecenie:

- angielski: `projects/<slug-ksiazki>/`
- inne języki: `projects/<slug-ksiazki>-<kod-jezyka>/`

Przykłady:

- `projects/qu-est-ce-que-le-fascisme-maurice-bardeche`
- `projects/qu-est-ce-que-le-fascisme-maurice-bardeche-pl`

To pozwala trzymać kilka wersji językowych tej samej książki obok siebie.

## Główne komendy

### 1. Inicjalizacja projektu

```cmd
python tools\epub_translate.py init-project --epub "Qu'est-ce que le fascisme _ - Maurice Bardeche.epub" --project-root projects --target-language pl
```

Tworzy:

- `project.json`
- `glossary.md`
- `QA.md`
- `workspace/`
- `batches/`

### 2. Lista plików treści

```cmd
python tools\epub_translate.py list-content --project qu-est-ce-que-le-fascisme-maurice-bardeche-pl --project-root projects
```

### 3. Konfiguracja OpenAI

```cmd
python tools\epub_translate.py configure-openai --project qu-est-ce-que-le-fascisme-maurice-bardeche-pl --project-root projects --model gpt-5.4 --reasoning-effort none --use-batch
```

Najważniejsze opcje:

- `--model`
- `--temperature`
- `--reasoning-effort {none,low,medium,high}`
- `--use-batch`
- `--no-use-batch`
- `--run-qa-after-apply`
- `--no-run-qa-after-apply`

Uwaga:

- dla `gpt-5.4` zwykle warto mieć `--reasoning-effort none`
- dla części modeli trzeba mieć `send_temperature=false`; skrypt obsługuje to w konfiguracji projektu

### 4. Szacunek kosztu

```cmd
python tools\epub_translate.py estimate-cost --project qu-est-ce-que-le-fascisme-maurice-bardeche-pl --project-root projects
```

To jest estymacja, nie gwarantowany koszt końcowy.

### 5. Szacunek kosztu QA w chmurze

```cmd
python tools\epub_translate.py estimate-qa-cost --project qu-est-ce-que-le-fascisme-maurice-bardeche-pl --project-root projects
```

To liczy lekki batch QA porównujący tekst źródłowy i tłumaczenie.

## Workflow batch

To jest domyślny, najtańszy i najwygodniejszy tryb dla całej książki.

### Jedna komenda: przygotowanie i wysłanie batcha

Cała książka:

```cmd
python tools\epub_translate.py run-batch --project qu-est-ce-que-le-fascisme-maurice-bardeche-pl --project-root projects
```

Jeden rozdział:

```cmd
python tools\epub_translate.py run-batch --project qu-est-ce-que-le-fascisme-maurice-bardeche-pl --project-root projects --chapter OEBPS/e9782307290681_c01.xhtml
```

### Sprawdzanie statusu

```cmd
python tools\epub_translate.py batch-status --project qu-est-ce-que-le-fascisme-maurice-bardeche-pl --project-root projects
```

Statusy typowe:

- `validating`
- `in_progress`
- `finalizing`
- `completed`
- `failed`

### Pobranie wyników

```cmd
python tools\epub_translate.py batch-download-output --project qu-est-ce-que-le-fascisme-maurice-bardeche-pl --project-root projects
```

### Zastosowanie wyników do EPUB

```cmd
python tools\epub_translate.py apply-batch-output --project qu-est-ce-que-le-fascisme-maurice-bardeche-pl --project-root projects --skip-qa
```

Po tym skrypt:

- aktualizuje `progress.json`
- wpisuje tłumaczenia do XHTML/HTML
- składa nowy plik EPUB

## Workflow cloud QA

To jest osobny batch diagnostyczny. Nie zmienia EPUB, tylko generuje raport problemów.

### Jedna komenda: przygotowanie i wysłanie QA batcha

```cmd
python tools\epub_translate.py run-qa-batch --project qu-est-ce-que-le-fascisme-maurice-bardeche-pl --project-root projects
```

Jeden rozdział:

```cmd
python tools\epub_translate.py run-qa-batch --project qu-est-ce-que-le-fascisme-maurice-bardeche-pl --project-root projects --chapter OEBPS/e9782307290681_c01.xhtml
```

### Status QA batcha

```cmd
python tools\epub_translate.py qa-batch-status --project qu-est-ce-que-le-fascisme-maurice-bardeche-pl --project-root projects
```

### Pobranie outputu QA

```cmd
python tools\epub_translate.py qa-batch-download-output --project qu-est-ce-que-le-fascisme-maurice-bardeche-pl --project-root projects
```

### Złożenie raportu QA

```cmd
python tools\epub_translate.py apply-qa-output --project qu-est-ce-que-le-fascisme-maurice-bardeche-pl --project-root projects
```

Wynik trafia do:

- `projects/<projekt>/QA_cloud.md`

## Workflow direct

Przydaje się do małych testów jakościowych.

Jeden rozdział:

```cmd
python tools\epub_translate.py translate-direct --project qu-est-ce-que-le-fascisme-maurice-bardeche-pl --project-root projects --chapter OEBPS/e9782307290681_c01.xhtml
```

Cała książka:

```cmd
python tools\epub_translate.py translate-direct --project qu-est-ce-que-le-fascisme-maurice-bardeche-pl --project-root projects
```

Tryb direct:

- robi zwykłe requesty do API
- jest wygodny do testów
- zwykle jest droższy niż batch

## Postęp i wznowienie

Stan zapisuje się w:

- `projects/<projekt>/workspace/progress.json`

Jeśli proces padnie albo przerwiesz go `Ctrl+C`, można wznowić tą samą komendą.

## Chunk size

Parametr projektu:

- `translation.max_chars_per_chunk`

Praktycznie:

- `3200` = bezpieczniej, ale więcej requestów
- `6400` = taniej i zwykle lepiej dla batcha

`max_chars_per_chunk` dotyczy tekstu książki, a nie całego requestu. Prompt i glosariusz są doliczane osobno.

## Glosariusz

Plik:

- `projects/<projekt>/glossary.md`

Wpisuj tam:

- nazwiska
- instytucje
- miejsca
- terminy historyczne i prawne

Skrypt dołącza glosariusz do promptu.

## QA i cleanup

Po tłumaczeniu warto zrobić dwa etapy:

1. lokalny QA
2. lokalny cleanup

Lokalny QA sprawdza:

- parsowanie XML/XHTML
- resztki starego tytułu
- stare referencje do okładki
- podstawowe problemy metadanych

Lokalny cleanup zwykle obejmuje:

- `dc:title`
- `<title>` w plikach XHTML
- `toc.ncx`
- `toc.xhtml`
- `guide`/`reference` w `.opf`
- podmianę okładki

## Typowe pliki projektu

- `project.json` — konfiguracja projektu
- `glossary.md` — glosariusz
- `QA.md` — raport jakości
- `workspace/unpacked/source/` — rozpakowany oryginał
- `workspace/unpacked/translated/` — rozpakowana wersja robocza po tłumaczeniu
- `workspace/progress.json` — postęp
- `batches/requests.jsonl` — wejście batcha
- `batches/output.jsonl` — odpowiedzi batcha

## Uwagi praktyczne

- batch kończy się szybko po stronie terminala, bo wysyła zadanie asynchroniczne do OpenAI
- tłumaczenie nie dzieje się lokalnie; lokalna jest tylko obróbka EPUB
- dla EPUBów po konwersji z PDF często trzeba po tłumaczeniu poprawić metadane i nawigację
- okładkę najlepiej podmieniać lokalnie po zakończeniu tłumaczenia
