/*
** 2018 February 19
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** This file contains code used for testing the SQLite system.
** None of the code in this file goes into a deliverable build.
**
** This file contains a stub implementation of the write-ahead log replication
** interface. It can be used by tests to exercise the WAL replication APIs
** exposed by SQLite.
**
** This replication implementation is designed for testability and does
** not involve any actual networking.
*/
#if defined(SQLITE_ENABLE_WAL_REPLICATION) && !defined(SQLITE_OMIT_WAL)

#if defined(INCLUDE_SQLITE_TCL_H)
#  include "sqlite_tcl.h"
#else
#  include "tcl.h"
#endif

#include "sqliteInt.h"
#include "sqlite3.h"
#include <assert.h>

extern const char *sqlite3ErrName(int);

/* These functions are implemented in test1.c. */
extern int getDbPointer(Tcl_Interp *, const char *, sqlite3 **);

/*
** A no-op version of sqlite3_wal_replication.xBegin().
*/
static int testWalReplicationBegin(
  sqlite3_wal_replication *pReplication, void *pArg
){
  return 0;
}

/*
** A no-op version of sqlite3_wal_replication.xAbort().
*/
static int testWalReplicationAbort(
  sqlite3_wal_replication *pReplication, void *pArg
){
  return 0;
}

/*
** A no-op version of sqlite3_wal_replication.xFrames().
*/
static int testWalReplicationFrames(
  sqlite3_wal_replication *pReplication, void *pArg,
  int szPage, int nFrame, sqlite3_wal_replication_frame *aFrame,
  unsigned nTruncate, int isCommit
){
  return 0;
}

/*
** A no-op version of sqlite3_wal_replication.xUndo().
*/
static int testWalReplicationUndo(
  sqlite3_wal_replication *pReplication, void *pArg
){
  return 0;
}

/*
** A no-op version of sqlite3_wal_replication.xEnd().
*/
static int testWalReplicationEnd(
  sqlite3_wal_replication *pReplication, void *pArg
){
  return 0;
}

/*
** This function returns a pointer to the WAL replication implemented in this
** file.
*/
sqlite3_wal_replication *testWalReplication(void){
  static sqlite3_wal_replication replication = {
    1,
    0,
    "test",
    0,
    testWalReplicationBegin,
    testWalReplicationAbort,
    testWalReplicationFrames,
    testWalReplicationUndo,
    testWalReplicationEnd,
  };
  return &replication;
}

/*
** This function returns a pointer to the WAL replication implemented in this
** file, but using a different registration name than testWalRepl.
**
** It's used to exercise the WAL replication registration APIs.
*/
sqlite3_wal_replication *testWalReplicationAlt(void){
  static sqlite3_wal_replication replication = {
    1,
    0,
    "test-alt",
    0,
    testWalReplicationBegin,
    testWalReplicationAbort,
    testWalReplicationFrames,
    testWalReplicationUndo,
    testWalReplicationEnd,
  };
  return &replication;
}

/*
** tclcmd: sqlite3_wal_replication_find ?NAME?
**
** Return the name of the default WAL replication implementation, if one is
** registered, or no result otherwise.
**
** If NAME is passed, return NAME if a matching WAL replication implementation
** is registered, or no result otherwise.
*/
static int SQLITE_TCLAPI test_wal_replication_find(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  char *zName;
  sqlite3_wal_replication *pReplication;

  if( objc!=1 && objc!=2 ){
    Tcl_WrongNumArgs(interp, 2, objv, "?NAME?");
    return TCL_ERROR;
  }

  if( objc==2 ){
    zName = Tcl_GetString(objv[1]);
  }

  pReplication = sqlite3_wal_replication_find(zName);

  if( pReplication ){
    Tcl_AppendResult(interp, pReplication->zName, (char*)0);
  }

  return TCL_OK;
}

/*
** tclcmd: sqlite3_wal_replication_register DEFAULT ?ALT?
**
** Register the test write-ahead log replication implementation, with the name
** "test", making it the default if DEFAULT is 1.
**
** If the ALT flag is true, use "test-alt" as registration name.
*/
static int SQLITE_TCLAPI test_wal_replication_register(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  int bDefault = 0;
  int bAlt = 0;
  sqlite3_wal_replication *pReplication;

  if( objc!=2 && objc!=3 ){
    Tcl_WrongNumArgs(interp, 3, objv, "DEFAULT ?ALT?");
    return TCL_ERROR;
  }

  if( Tcl_GetIntFromObj(interp, objv[1], &bDefault) ){
    return TCL_ERROR;
  }

  if( objc==3 ){
    if( Tcl_GetIntFromObj(interp, objv[2], &bAlt) ){
      return TCL_ERROR;
    }
  }

  if( bAlt==0 ){
    pReplication = testWalReplication();
  }else{
    pReplication = testWalReplicationAlt();
  }

  sqlite3_wal_replication_register(pReplication, bDefault);

  return TCL_OK;
}

/*
** tclcmd: sqlite3_wal_replication_unregister ?ALT?
**
** Unregister the test write-ahead log replication implementation.
**
** If the ALT flag is true, unregister the alternate implementation.
*/
static int SQLITE_TCLAPI test_wal_replication_unregister(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  int bAlt = 0;

  if( objc!=1 && objc!=2 ){
    Tcl_WrongNumArgs(interp, 2, objv, "?ALT?");
    return TCL_ERROR;
  }

  if( objc==2 ){
    if( Tcl_GetIntFromObj(interp, objv[1], &bAlt) ){
      return TCL_ERROR;
    }
  }

  if( bAlt==0 ){
    sqlite3_wal_replication_unregister(testWalReplication());
  }else{
    sqlite3_wal_replication_unregister(testWalReplicationAlt());
  }
  return TCL_OK;
}

/*
** tclcmd: sqlite3_wal_replication_enabled HANDLE SCHEMA
**
** Return "true" if WAL replication is enabled on the given database, "false"
** otherwise.
**
** If leader replication is enabled, the name of the implementation used is also
** returned.
*/
static int SQLITE_TCLAPI test_wal_replication_enabled(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  int rc;
  sqlite3 *db;
  const char *zSchema;
  int bEnabled;
  sqlite3_wal_replication *pReplication;
  char *zEnabled;
  const char *zReplication = 0;
  char zBuf[32];

  if( objc!=3 ){
    Tcl_WrongNumArgs(interp, 1, objv, "HANDLE SCHEMA");
    return TCL_ERROR;
  }

  if( getDbPointer(interp, Tcl_GetString(objv[1]), &db) ){
    return TCL_ERROR;
  }
  zSchema = Tcl_GetString(objv[2]);

  rc = sqlite3_wal_replication_enabled(db, zSchema, &bEnabled, &pReplication);

  if( rc!=SQLITE_OK ){
    Tcl_AppendResult(interp, sqlite3ErrName(rc), (char*)0);
    return TCL_ERROR;
  }

  if( bEnabled ){
    zEnabled = "true";
    if( pReplication ){
      zReplication = pReplication->zName;
    }
  }else{
    zEnabled = "false";
  }

  if( zReplication ){
    sqlite3_snprintf(sizeof(zBuf), zBuf, " %s", zReplication);
  }else{
    zBuf[0] = 0;
  }

  Tcl_AppendResult(interp, zEnabled, zBuf, (char*)0);

  return TCL_OK;
}

/*
** tclcmd: sqlite3_wal_replication_leader HANDLE SCHEMA ?NAME?
**
** Enable leader WAL replication for the given connection/schema, using the stub
** WAL replication implementation defined in this file, or the one registered
** under NAME if given.
*/
static int SQLITE_TCLAPI test_wal_replication_leader(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  int rc;
  sqlite3 *db;
  const char *zSchema;
  const char *zReplication = "test";

  if( objc!=3 && objc!=4 ){
    Tcl_WrongNumArgs(interp, 4, objv, "HANDLE SCHEMA ?NAME?");
    return TCL_ERROR;
  }

  if( getDbPointer(interp, Tcl_GetString(objv[1]), &db) ){
    return TCL_ERROR;
  }
  zSchema = Tcl_GetString(objv[2]);

  if( objc==4 ){
    zReplication = Tcl_GetString(objv[3]);
  }


  rc = sqlite3_wal_replication_leader(db, zSchema, zReplication, 0);

  if( rc!=SQLITE_OK ){
    Tcl_AppendResult(interp, sqlite3ErrName(rc), (char*)0);
    return TCL_ERROR;
  }

  return TCL_OK;
}

/*
** tclcmd: sqlite3_wal_replication_follower HANDLE SCHEMA
**
** Enable follower WAL replication for the given connection/schema.
*/
static int SQLITE_TCLAPI test_wal_replication_follower(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  int rc;
  sqlite3 *db;
  const char *zSchema;

  if( objc!=3 ){
    Tcl_WrongNumArgs(interp, 3, objv, "HANDLE SCHEMA");
    return TCL_ERROR;
  }

  if( getDbPointer(interp, Tcl_GetString(objv[1]), &db) ){
    return TCL_ERROR;
  }
  zSchema = Tcl_GetString(objv[2]);

  rc = sqlite3_wal_replication_follower(db, zSchema);

  if( rc!=SQLITE_OK ){
    Tcl_AppendResult(interp, sqlite3ErrName(rc), (char*)0);
    return TCL_ERROR;
  }

  return TCL_OK;
}

/*
** This routine registers the custom TCL commands defined in this
** module.  This should be the only procedure visible from outside
** of this module.
*/
int Sqlitetestwalreplication_Init(Tcl_Interp *interp){
  Tcl_CreateObjCommand(interp, "sqlite3_wal_replication_find",
          test_wal_replication_find,0,0);
  Tcl_CreateObjCommand(interp, "sqlite3_wal_replication_register",
          test_wal_replication_register,0,0);
  Tcl_CreateObjCommand(interp, "sqlite3_wal_replication_unregister",
          test_wal_replication_unregister,0,0);
  Tcl_CreateObjCommand(interp, "sqlite3_wal_replication_enabled",
          test_wal_replication_enabled,0,0);
  Tcl_CreateObjCommand(interp, "sqlite3_wal_replication_leader",
          test_wal_replication_leader,0,0);
  Tcl_CreateObjCommand(interp, "sqlite3_wal_replication_follower",
          test_wal_replication_follower,0,0);
  return TCL_OK;
}
#endif /* SQLITE_ENABLE_WAL_REPLICATION */
