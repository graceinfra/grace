//DELDSN   JOB CLASS=A,MSGCLASS=X,NOTIFY=&SYSUID
//STEP1    EXEC PGM=IDCAMS
//SYSPRINT DD SYSOUT=*
//SYSIN    DD *
  DELETE IBMUSER.GRC.H1HGFRP.PARAMDAT -
    FILE(TEMPDSN) PURGE
/*
//TEMPDSN  DD DSN=IBMUSER.GRC.H1HGFRP.PARAMDAT,
//            UNIT=SYSALLDA,VOL=SER=USRVS1,DISP=OLD
