# Big Data Processing

**Big Data** označuje obrovské objemy dát, ktoré sú tak rozsiahle a komplexné, že ich nemožno efektívne spracovať tradičnými databázovými nástrojmi. Tieto dáta pochádzajú z rôznych zdrojov, ako sú sociálne médiá, internetové stránky, senzory, podnikové systémy či mobilné zariadenia.

#### Hlavné charakteristiky Big Data (3V model):

##### Volume (Objem):
- Dáta sú generované vo veľkých objemoch, často v petabajtoch alebo exabajtoch.

##### Velocity (Rýchlosť): 
- Dáta sú vytvárané a spracovávané v reálnom čase alebo s minimálnym oneskorením.

##### Variety (Rôznorodosť):
- Dáta môžu byť štruktúrované (tabuľky, databázy), neštruktúrované (texty, obrázky, videá) alebo pološtruktúrované (JSON, XML).

**Rozšírené modely pridávajú aj ďalšie faktory:** 
- _Veracity_ (pravdivosť dát),
- _Value_ (hodnota dát).

## Dáta s ktorými pracujeme:
Záznamy popisujúce dopravné nehody vo Veľkej Británii z obdobia od roku 2005 do 2014. Dáta sú rozdelené do troch súborov:

- Accidents: dátový súbor obsahujúci informácie o nehodách. Každý záznam reprezentuje jednu nehodu a popisuje rôzne informácie o jej závažnosti, počasí, geolokácii, čase atď.
- Vehicles: dátový súbor obsahujúci informácie o vozidlách, ktoré sa zúčastnili danej nehody (typ vozidla, motor, veľkosť, údaje o vodičovi atď.)
- Casualties: dátový súbor obsahujúci informácie o obetiach - závažnosť zranení, vek, pohlavie, atď.

Dáta sú v CSV súboroch. Atribút "Accident_Index" môže byť použitý ako jednoznačný identifikátor nehody, ktorý je možné použiť pre prepojenie informácií o vozidlách, ktoré sa zúčastnili nehody, informácií o samotnej nehode a obetiach. Údaje obsahujúce “-1” sú pre chýbajúce hodnoty, resp. hodnoty mimo rozsah. Hodnoty jednotlivých atribútov sú zakódované, významy jednotlivých hodnôt atribútov nájdete v súbore atributy.xls.

## Čo sme spravili:
#### Integrácia dát:
- Integrácia datasetu - vhodne zakomponujte zvolené informácie o počasí.
- Sampling – vytvorenie vzorky z datasetu (veľkosti napr. 10%) pri zachovaní rozloženia cieľového atribútu.
- Rozdelenie datasetu na trénovaciu a testovaciu množinu (napr. v pomere 60/40).

#### Transformácia dát:
- Transformácia nominálnych atribútov na numerické
- Transformácia numerických atribútov na nominálne
- Vypočítanie pomerového kritéria – informačného zisku voči cieľovému atribútu (klasifikačná úloha), pre nominálne atribúty
- Vypočítanie štatistík pre numerické atribúty
- Vytvorenie histogramov pre nominálne atribúty
- Spracovanie chýbajúcich hodnôt (napr. ich nahradenie priemermi, atď.)


#### Modelovanie - Vytvorenie popisných modelov:
- Vytvorte k-means clustering model
- Pomocou vytvoreného modelu detekujte anomálie


#### Modelovanie - Vytvorenie klasifikačných modelov typu:
- Decision tree model
- Linear SVM
- Naive Bayes model
- Ensembles of decision trees (Random Forests, Gradient-boosted trees)

#### Vyhodnotenie:

- Natrénovanie klasifikačného modelu na trénovacej množine a jeho evaluáciu na testovacej množine.
- Klasifikačný model vyhodnocujte použitím kontigenčnej tabuľky a vypočítaním metrík presnosti, návratnosti, F1 a MCC (Matthews Correlation Coefficient).

## Čo sme použili:
#### Apache Spark:
- Apache Spark je open-source distribuovaný výpočtový rámec, ktorý umožňuje rýchle spracovanie veľkých objemov dát. Je navrhnutý tak, aby bol rýchly, flexibilný a jednoduchý na použitie.

#### MLlib:
- je knižnica pre strojové učenie v Apache Sparku, ktorá poskytuje nástroje na vytváranie a hodnotenie modelov strojového učenia. Obsahuje algoritmy pre klasifikáciu, regresiu, zhlukovanie a ďalšie úlohy strojového učenia.