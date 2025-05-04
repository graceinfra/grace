       IDENTIFICATION DIVISION.
       PROGRAM-ID. VSAMCSV.
       AUTHOR. Arnav Surve.

       ENVIRONMENT DIVISION.
       INPUT-OUTPUT SECTION.
       FILE-CONTROL.
           SELECT VSAM-IN ASSIGN TO VSAMIN
                  ORGANIZATION IS INDEXED
                  ACCESS MODE IS SEQUENTIAL
                  RECORD KEY IS VSAM-EMPL-ID
                  FILE STATUS IS WS-VSAM-STATUS.

           SELECT CSV-OUT ASSIGN TO CSVOUT
                  ORGANIZATION IS SEQUENTIAL
                  ACCESS MODE IS SEQUENTIAL
                  FILE STATUS IS WS-CSV-STATUS.

       DATA DIVISION.
       FILE SECTION.

       FD VSAM-IN
           RECORD CONTAINS 80 CHARACTERS
           BLOCK CONTAINS 0 RECORDS.
       01 VSAM-REC.
          05 VSAM-EMPL-ID      PIC X(5).
          05 VSAM-EMPL-NAME    PIC X(26).
          05 VSAM-EMPL-SALARY  PIC X(10).
          05 FILLER            PIC X(35).

       FD CSV-OUT
           RECORD CONTAINS 80 CHARACTERS
           BLOCK CONTAINS 0 RECORDS.
       01 CSV-REC               PIC X(80).

       WORKING-STORAGE SECTION.

       01 WS-FILE-STATUS.
          05 WS-VSAM-STATUS    PIC XX  VALUE SPACES.
             88 VSAM-OK              VALUE '00'.
             88 VSAM-EOF             VALUE '10'.
          05 WS-CSV-STATUS     PIC XX  VALUE SPACES.
             88 CSV-OK               VALUE '00'.

       01 WS-FLAGS.
          05 WS-EOF-FLAG       PIC X   VALUE 'N'.
             88 EOF-REACHED          VALUE 'Y'.

       01 WS-COUNTERS.
          05 WS-RECORDS-READ    PIC 9(7) VALUE 0.
          05 WS-RECORDS-WRITTEN PIC 9(7) VALUE 0.

       01 WS-CSV-HEADER         PIC X(80) VALUE
             'EMPL_ID,EMPL_NAME,EMPL_SALARY'.

       01 WS-SALARY-NUMERIC     PIC 9(5)V99 COMP-3.
       01 WS-SALARY-FORMATTED   PIC 9(5).99.

       01 WS-EMPL-ID-NUMERIC    PIC 9(5).
       01 WS-EMPL-ID-FORMATTED  PIC Z(5).

       PROCEDURE DIVISION.
       MAIN-PROCEDURE.
           PERFORM 1000-INITIALIZE
           PERFORM 2000-PROCESS-RECORDS UNTIL EOF-REACHED
           PERFORM 3000-TERMINATE
           STOP RUN.

       1000-INITIALIZE.
           PERFORM 1100-OPEN-FILES
           PERFORM 1200-WRITE-HEADER
           PERFORM 1300-READ-VSAM.

       1100-OPEN-FILES.
           OPEN INPUT VSAM-IN
           IF NOT VSAM-OK
              DISPLAY 'ERROR OPENING VSAM FILE, STATUS: ' WS-VSAM-STATUS
              MOVE 16 TO RETURN-CODE
              STOP RUN
           END-IF.

           OPEN OUTPUT CSV-OUT
           IF NOT CSV-OK
              DISPLAY 'ERROR OPENING CSV FILE, STATUS: ' WS-CSV-STATUS
              MOVE 16 TO RETURN-CODE
              STOP RUN
           END-IF.

       1200-WRITE-HEADER.
           MOVE WS-CSV-HEADER TO CSV-REC
           WRITE CSV-REC
           IF NOT CSV-OK
              DISPLAY 'ERROR WRITING CSV HEADER, STATUS: ' WS-CSV-STATUS
              MOVE 16 TO RETURN-CODE
              STOP RUN
           END-IF.

       1300-READ-VSAM.
           READ VSAM-IN NEXT RECORD
               AT END MOVE 'Y' TO WS-EOF-FLAG
           END-READ

           IF NOT EOF-REACHED AND NOT VSAM-OK
              DISPLAY 'ERROR READING VSAM FILE, STATUS: ' WS-VSAM-STATUS
              MOVE 16 TO RETURN-CODE
              STOP RUN
           END-IF

           IF NOT EOF-REACHED
              ADD 1 TO WS-RECORDS-READ
           END-IF.

       2000-PROCESS-RECORDS.
           PERFORM 2100-FORMAT-CSV-LINE
           PERFORM 2200-WRITE-CSV-LINE
           PERFORM 1300-READ-VSAM.

       2100-FORMAT-CSV-LINE.
           INITIALIZE CSV-REC

           *> Format salary
           IF VSAM-EMPL-SALARY = SPACES
               MOVE 0 TO WS-SALARY-NUMERIC
           ELSE
               COMPUTE WS-SALARY-NUMERIC =
                   FUNCTION NUMVAL(VSAM-EMPL-SALARY) / 100
           END-IF
           MOVE WS-SALARY-NUMERIC TO WS-SALARY-FORMATTED

           *> Format ID with leading zeroes
           COMPUTE WS-EMPL-ID-NUMERIC = FUNCTION NUMVAL(VSAM-EMPL-ID)
           MOVE WS-EMPL-ID-NUMERIC TO WS-EMPL-ID-FORMATTED

           STRING
               FUNCTION TRIM(WS-EMPL-ID-FORMATTED LEADING)
                                              DELIMITED BY SIZE
               ','                            DELIMITED BY SIZE
               FUNCTION TRIM(VSAM-EMPL-NAME)  DELIMITED BY SIZE
               ','                            DELIMITED BY SIZE
               WS-SALARY-FORMATTED            DELIMITED BY SIZE
               INTO CSV-REC
           END-STRING.

       2200-WRITE-CSV-LINE.
           WRITE CSV-REC
           IF NOT CSV-OK
              DISPLAY 'ERROR WRITING CSV RECORD, STATUS: ' WS-CSV-STATUS
              MOVE 16 TO RETURN-CODE
              STOP RUN
           END-IF
           ADD 1 TO WS-RECORDS-WRITTEN.

       3000-TERMINATE.
           PERFORM 3100-CLOSE-FILES
           PERFORM 3200-DISPLAY-SUMMARY.

       3100-CLOSE-FILES.
           CLOSE VSAM-IN
           IF NOT VSAM-OK
              DISPLAY 'CLOSING VSAM FILE, STATUS: ' WS-VSAM-STATUS
           END-IF.

           CLOSE CSV-OUT
           IF NOT CSV-OK
              DISPLAY 'CLOSING CSV FILE, STATUS: ' WS-CSV-STATUS
           END-IF.

       3200-DISPLAY-SUMMARY.
           DISPLAY 'VSAM to CSV processing complete.'
           DISPLAY 'Records Read:    ' WS-RECORDS-READ
           DISPLAY 'Records Written: ' WS-RECORDS-WRITTEN.

       END PROGRAM VSAMCSV.
