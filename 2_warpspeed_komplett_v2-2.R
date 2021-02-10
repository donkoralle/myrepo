# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#    WarpSpeed-Diagramm basteln
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Hinweise:
# * Code wurde auf einem Win10-System mit R version 4.0.3 (2020-10-10) -- "Bunny-Wunnies Freak Out" erstellt
# * Code wurde für einen Aufruf über die Windows-Eingabeaufforderung & Rscript.exe ausgelegt (v.a. print), um das tägliche Ausführen zu erleichtern 


# ==== Let's roll ====

library(tidyverse)
library(scales)
library(extrafont)
library(RColorBrewer)
library(RSQLite)
library(lubridate)

setwd("~/1___Work/R/corona_impfungen")
ausgangsjahr <- 2021
Immunisierungsquote <- 0.7
myDB <- "Data/impfen_v1.db"
myCSVexportFile <- "Data/timeline_extrapol.csv" 


# ==== tagesaktuelle Daten vom BMSGPK laden ====
# https://info.gesundheitsministerium.at/opendata.html
# Aktualisierung: Die Aktualisierung erfolgt täglich gegen 14 Uhr (Stand 05.02.21 - https://www.data.gv.at/katalog/dataset/589132b2-c000-4c60-85b4-c5036cdf3406)

print("\n### Daten abrufen ... ###\n")

direktImport <- as_tibble(read.csv2("https://info.gesundheitsministerium.at/data/timeline.csv",
                                    encoding = "UTF-8",
                                    dec = "."))

# Sicherheitshalber: UTF BOM ausbessern (Why-oh-why muss das sein?) & Datum umwandeln
if(any(grepl("X.U.FEFF.", colnames(direktImport)))){

  print("\n### ... UTF-8 BOM erkannt & bereinigt. ###\n")
  
  direktImport <- direktImport %>%
    rename(., Datum = X.U.FEFF.Datum)
}

# Datum in Date umwandeln
direktImport <- direktImport %>%
  mutate(Datum = as.Date(Datum))
glimpse(direktImport)


# ==== Db-Verbindung erstellen ====

# Vebindung erstellen
conn <- dbConnect(SQLite(), myDB)

# max. Datum aus DB abfragen
max.DBdatum <- dbGetQuery(conn, "SELECT MAX(Datum) FROM timeline")[1,1]

paste("\n### Aktuellste Einträge in DB vom:",
      max.DBdatum,
      "###\n")

# ausfiltern der in die DB zu übertragenden Records
new.data <- direktImport %>%
  filter(Datum > max.DBdatum)


# ==== Mainloop ====


if(nrow(new.data) == 0){
  
  # ==== Nebenast: keine neue Daten & exit ====

  print("\n### KEINE neuen Daten gefunden. ###\n")
  
} else {
  
  # ==== Hauptast: Daten schreiben ====
  
  print("\n### NEUE Daten gefunden: ###\n")
  
  new.data
  # sollten 10 Records je Tag sein
  
  # on-exit-Konversion des Datums
  new.data <- new.data %>%
    mutate(Datum = as.character(Datum))
  
  # bevor: n vor dem Update ermitteln
  paste("\n### n Records vor DB-Update:",
        dbGetQuery(conn, "SELECT COUNT(*) FROM timeline"),
        "###\n"
  )
  # neue Records in DB schreiben
  dbWriteTable(conn,"timeline", new.data, append = TRUE)
  # danach: n nach Update ermitteln
  paste("\n### n Records nach DB-Update:",
        dbGetQuery(conn, "SELECT COUNT(*) FROM timeline"),
        "###\n"
  )
  
      # # Blick auf die neue DB-Daten
      # print("\n### Die aktuellesten Einträge aus der DB: ###\n")
      # 
      # head(dbGetQuery(conn, "SELECT * FROM timeline ORDER BY Datum desc"))
  
  
  # ==== Diagramm ONLY: Daten aus lokaler DB holen ====
  
  # max. Datum aus DB abfragen
  myTimewindow <- 14
  max.DBdatum <- dbGetQuery(conn, "SELECT MAX(Datum) FROM timeline")[1,1]
  max.DBdatumMinus14plus6 <- as.Date(max.DBdatum)-(myTimewindow+6)
  
  # SQL-Statement zur Datenabfrage basteln
  sql_1 <- paste("SELECT * FROM timeline WHERE Datum BETWEEN date('", 
                 max.DBdatumMinus14plus6, 
                 "') and date('", 
                 max.DBdatum, 
                 "') AND BundeslandID < 10 ORDER BY Datum, BundeslandID",
                 sep = "")
  sql_1
  
  # DB-Daten abfragen & on-entry-Datumskonversion & Konversion bula
  raw.daten <- dbGetQuery(conn, sql_1) %>%
    as_tibble(.) %>%
    mutate(Datum = as.Date(Datum),
           bula = as_factor(Name))
  raw.daten
  
    
  # ==== Impfraten ermitteln & extrapolieren ====
  
  print("\n### Impfraten ermitteln & extrapolieren ###\n")
  
  sel.daten <- raw.daten %>%
    select(Datum,
           bula,
           Bevölkerung,
           EingetrageneImpfungen,
           Teilgeimpfte,
           Vollimmunisierte) %>%
    arrange(desc(Datum)) %>%
    mutate(Datum_minus7 = lead(Datum, n = 9*7),
           vollimmun_minus7 = lead(Vollimmunisierte, n = 9*7),
           delta_vollimm = Vollimmunisierte - vollimmun_minus7,
           vollimm_jeTag_last7 = delta_vollimm / 7,
           DeltaNonImmBev = round(Bevölkerung * Immunisierungsquote, 0) - Vollimmunisierte,
           dauerDeltaNonimmBev = round(DeltaNonImmBev/vollimm_jeTag_last7, 0),
           TagErreichenImmunquote = Datum + dauerDeltaNonimmBev
    )
  
  # sicherheitshalber: Es könnten Divisionen durch 0 aufgetreten sein (v.a. am Beginn der Zeitreihe) > Inf durch NA ersetzen
  sel.daten <- sel.daten %>% 
    mutate_if(is.numeric, list(~na_if(., Inf))) %>% 
    mutate_if(is.numeric, list(~na_if(., -Inf)))
  head(sel.daten, 20)
  
  # sel.daten noch als CSV-File exportieren
  # V1
  # write.csv2(sel.daten, myCSVexportFile, fileEncoding = "UTF-8")
  # liefert nur die letzten 14 Tage
  # V2: Dump aus DB
  myCSVexportFrame <- dbGetQuery(conn, "SELECT * FROM timeline_extrapol ORDER BY Datum DESC")
  # glimpse(myCSVexportFrame)
  write.csv2(myCSVexportFrame, myCSVexportFile, fileEncoding = "UTF-8")
  
  
  # ==== Impfraten in lokaler DB ablegen ====
  max.DBextrapolDatum <- dbGetQuery(conn, "SELECT MAX(Datum) FROM timeline_extrapol")[1,1]  

  # abzulegende extrapolierten Daten ermitteln & für Export vorbereiten
  new.data.extrapol <- sel.daten %>%
    filter(Datum > max.DBextrapolDatum) %>%
    mutate_if(lubridate::is.Date, ~ as.character(.))
  glimpse(new.data.extrapol)
  
  # Checken, ob Daten abgelegt werden müssen
  if(nrow(new.data.extrapol) == 0){
    print("\n ### Keine neuen extrapolierten Daten lokal abzulegen ###\n")
  }else{
    print("\n### NEUE extrapolierte Daten lokal ablegen ###\n")
    
    # Daten ablegen in DB
    dbWriteTable(conn,"timeline_extrapol", new.data.extrapol, append = TRUE)
  }
  
  # ==== Vis-Daten erzeugen ====
  
  print("\n### Digramm erstellen ... ###\n")
  
  # Filtern, um NA zu beseitigen über Datum da ja nur 14 Tage dargestellt werden sollen
  maxVisDatum <- max(sel.daten$Datum)-14
  vis.daten1 <- sel.daten %>%
    filter(Datum > maxVisDatum)
  glimpse(vis.daten1)
  
  # ==== Visualisieren ====
  
  myTitle <- paste("Wann würden die Bundesländer eine Impfquote von",
                   Immunisierungsquote*100,
                   "% erreichen?")
  mySubTitle <- "(ausgehend von den Vollimmunisierungen der jeweils letzten sieben Tage)"
  myCaption <- paste("\nStand: ",
                     max.DBdatum,
                     "\nDatenquelle: BMSGPK, Österreichisches COVID-19 Open Data Informationsportal\n(https://www.data.gv.at/covid-19)",
                     sep = "")
  yLabel <- paste("Impfquote (",
                  Immunisierungsquote*100,
                  " %) erreicht im Jahr ...\n",
                  sep=""
                  )
  
  # X-Dimension Visualisierung gesamt
  # Impfbeginn AUT: 27.12.20 plus 3 Wochen > 17.01.21
  myVisXstart <- as.Date("2020-12-27")+(3*7)
  myVisXend <- max(vis.daten1$Datum)+10
  # Y-Dimension Visualisierung gesamt
  myVisYmin <- as.Date("2020-01-01")
  myVisYmax <- as.Date("2029-12-31")
  # y Variablen für die Hotzones (heuer & nächstes Jahr)
  myThisYearMin <- as.Date("2021-01-01")
  myThisYearMax <- myThisYearMin + months(12) - days(1)
  myNextYearMin <- myThisYearMin + months(12)
  myNextYearMax <- myNextYearMin + months(12) - days(1)
  # Hotozone-Koordinaten
  hotzone.coord <- data.frame(xmin = c(myVisXstart, myVisXstart),
                              xmax = c(myVisXend, myVisXend),
                              ymin = c(myThisYearMin, myNextYearMin),
                              ymax = c(myThisYearMax, myNextYearMax),
                              zonename = c("dieses Jahr", "nächstes Jahr"))
  
  ggplot(vis.daten1, aes(x = Datum, y = TagErreichenImmunquote)) +
    geom_line(aes(color = bula), size = 1.1) +
    labs(x = "\nDatum", 
         y = yLabel,
         title = myTitle,
         subtitle = mySubTitle,
         caption = myCaption) +
    theme_gray() +
    theme(text = element_text(size=12, family="Calibri"),
          legend.text = element_text(size=10),
          legend.title = element_text(size=10, face = "bold"),
          legend.key.size = unit(1, 'lines'),
          plot.title = element_text(size=14, hjust = 0.5, face= "bold",
                                    margin=margin(7,0,0,0)),
          plot.subtitle = element_text(hjust = 0.5,
                                    margin=margin(5,0,10,0)),
          plot.caption = element_text(size=7, hjust = 0.5, face= "italic"),
          axis.title = element_text(face = "bold"),
          axis.text.x = element_text(angle = 90, vjust = 0.5),
          axis.ticks.length=unit(.15, "cm"),
          panel.border = element_rect(color = "black", fill = NA),
          panel.grid.minor.y = element_line(linetype = "solid")
    ) +
    scale_color_brewer(palette = "Spectral", name = "Bundesländer") +
    scale_x_date(date_breaks = "2 days", date_labels = "%d.%m") +
    scale_y_date(minor_breaks = "1 years") +
    geom_rect(hotzone.coord,
              mapping = aes(xmin=xmin,ymin=ymin,xmax=xmax,ymax=ymax,fill=zonename),
              alpha=c(0.3, 0.1),
              color=c("red", NA),
              inherit.aes=FALSE) +
    scale_fill_manual(name="Referenzzeiträume", 
                      labels = c("nächstes Jahr (2022)", "dieses Jahr (2021)"),
                      values=alpha(c("red","red"), c(0.1, 0.3))) +
    coord_cartesian(ylim = c(myVisYmin, myVisYmax),
                    xlim = c(min(vis.daten1$Datum), max(vis.daten1$Datum)))
  
  
  # ==== Diagramm speichern ====
  myFilename <- paste("Output/", 
                      max.DBdatum, 
                      "_warpspeed.png",
                      sep = "")
  ggsave(myFilename, 
         width = 8, height = 5, 
         dpi = 200, units = "in")  
  
  print("\n### Diagramm gespeichert ###\n")
}

# DB Verbindung lösen
dbDisconnect(conn)

# finales Feedback
print("\n### Ende ###")






