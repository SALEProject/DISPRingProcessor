unit MainDISPRingProcessor;

{$mode objfpc}{$H+}

interface

uses
 Classes, SysUtils, FileUtil, Forms, Controls, Graphics, Dialogs, StdCtrls,
 Spin, ExtCtrls, DISPProcessor, DISPRingProcessorVars, db;

type TProcessorScheduleRec = record
                              Active: boolean;
                              ID_Asset: integer;
                              LastCheck: TDateTime;
                              Timeout: integer;
                              DueDate: TDateTime;
                             end;
     TProcessorSchedule = array[0..100] of TProcessorScheduleRec;

type

 { TForm1 }

 TForm1 = class(TForm)
  ed_Exceptions: TMemo;
  ed_MatchingInterval: TSpinEdit;
  Label1: TLabel;
  Label2: TLabel;
  lbl_TimeOfRequest: TLabel;
  tmr: TTimer;
  procedure ed_MatchingIntervalChange(Sender: TObject);
  procedure FormCreate(Sender: TObject);
  procedure tmrTimer(Sender: TObject);
 private
  { private declarations }
 public
  { public declarations }

  processor: TDISPProcessor;
  schedule: TProcessorSchedule;
  schedule_count: integer;
  f_millisecond: double;

  procedure InitSchedule;
  function IndexOfSchedule(ID_Asset: integer): integer; inline;
 end;

var
 Form1: TForm1;

implementation

{$R *.lfm}

{ TForm1 }

procedure TForm1.ed_MatchingIntervalChange(Sender: TObject);
begin
 tmr.Interval:= ed_MatchingInterval.Value;
end;

procedure TForm1.FormCreate(Sender: TObject);
var StatusFileName, SettingsFileName: shortstring;
begin
 SettingsFileName:= ParamStr(0);
 SettingsFileName:= IncludeTrailingPathDelimiter(ExtractFilePath(SettingsFileName)) +
                    ExtractFileNameOnly(SettingsFileName) + '.xml';
 if FileExists(SettingsFileName) then Settings:= LoadGNRingProcessorSettings(SettingsFileName)
 else Application.Terminate;

 InitDS;

 ed_MatchingInterval.Value:= tmr.Interval;

{ StatusFileName:= ParamStr(0);
 StatusFileName:= IncludeTrailingPathDelimiter(ExtractFilePath(StatusFileName)) +
                  ExtractFileNameOnly(StatusFileName) + '_status.xml';
 processor:= TDISPProcessor.Create(StatusFileName);
 processor.DataSource:= ds_brm;
 processor.BursaryCode:= 'DISPONIBIL';}

 f_millisecond:= 1 / 86400 / 1000;
 InitSchedule;
end;

procedure TForm1.InitSchedule;
var ds_sessions: TDataset;
    ID_Ring, ID_Asset: integer;
    ID_RingSession, ID_AssetSession: integer;
    ID_InitialOrder: integer;
begin
 schedule_count:= 0;

 ds_sessions:= nil;
 try
  ds_sessions:= ds_brm['brm'].Query('SELECT M."ID" AS "ID_Market", R."ID" AS "ID_Ring", A."ID" AS "ID_Asset", A."ID_InitialOrder", ' +
                                    'RS."Status" AS "RingStatus", RS."ID" AS "ID_RingSession", S."ID" AS "ID_AssetSession", S."Status" AS "AssetStatus" ' +
                                    'FROM "Markets" M ' +
                                    'LEFT JOIN "Rings" R ON (M."ID" = R."ID_Market") ' +
                                    'RIGHT JOIN "Assets" A ON (R."ID" = A."ID_Ring") ' +
                                    'LEFT JOIN "RingSessions" RS ON (R."ID" = RS."ID_Ring") AND (CONVERT(varchar, RS."Date", 102) = CONVERT(varchar, GetDate(), 102)) ' +
                                    'LEFT JOIN "AssetSessions" S ON (A."ID" = S."ID_Asset") AND (CONVERT(varchar, S."Date", 102) = CONVERT(varchar, GetDate(), 102)) ' +
                                    'WHERE (M."ID_Bursary" = ' + IntToStr(cns_int_ID_Bursary) + ') AND (R."isActive" = 1) AND (A."isActive" = 1) ' +
                                    'AND (S."Status" IN (''Opened'', ''PreClosed''))');
  if ds_sessions = nil then Exit;
  if ds_sessions.IsEmpty then Exit;

  ds_sessions.First;
  while not ds_sessions.Eof do
   begin
    ID_Ring:= ds_sessions.FieldByName('ID_Ring').AsInteger;
    ID_Asset:= ds_sessions.FieldbyName('ID_Asset').AsInteger;
    ID_RingSession:= ds_sessions.FieldByName('ID_RingSession').AsInteger;
    ID_AssetSession:= ds_sessions.FieldByName('ID_AssetSession').AsInteger;
    ID_InitialOrder:= ds_sessions.FieldByName('ID_InitialOrder').AsInteger;

    schedule[schedule_count].Active:= true;
    schedule[schedule_count].ID_Asset:= ID_Asset;
    schedule[schedule_count].LastCheck:= Now;
    schedule[schedule_count].Timeout:= 0;
    schedule[schedule_count].DueDate:= Now;
    schedule_count += 1;

    ds_sessions.Next;
   end;
 finally
  FreeAndNil(ds_sessions);
 end;

end;

function TForm1.IndexOfSchedule(ID_Asset: integer): integer; inline;
 var b: boolean;
     i: integer;
 begin
  result:= -1;
  b:= false;
  i:= -1;
  while not b and (i < schedule_count - 1) do
   begin
    i += 1;
    if schedule[i].ID_Asset = ID_Asset then b:= true;
   end;

  if b then result:= i;
 end;

var inTimer: boolean;
procedure TForm1.tmrTimer(Sender: TObject);
var ds: TDataSet;
    i, k: integer;
    ID_Schedule, ID_Asset: integer;
    ScheduleDate, DueDate: TDateTime;
    Timeout: integer;
    s: string;
begin
 if inTimer then Exit;
 inTimer:= true;

 try
  //  check the Processor Due table in DB and update internal table
  s:= '';
  ds:= nil;
  try
   ds:= ds_brm['brm'].Query('SELECT * FROM "ProcessorSchedule" WHERE "Checked" = 0');
   if ds = nil then Exit;
   //if not ds.IsEmpty then lbl_TimeOfRequest.Caption:= DateTimeToStr(ds.FieldByName('DBNow').AsDateTime);

   ds.First;
   while not ds.EOF do
    begin
     ID_Schedule:= ds.FieldByName('ID').AsInteger;
     ID_Asset:= ds.FieldByName('ID_Asset').AsInteger;
     ScheduleDate:= ds.FieldByName('Date').AsDateTime;
     Timeout:= ds.FieldByName('Timeout').AsInteger;
     DueDate:= ds.FieldByName('DueDate').AsDateTime;

     k:= IndexOfSchedule(ID_Asset);
     if k = -1 then
      begin
       k:= schedule_count;
       schedule_count += 1;

       schedule[k].Active:= true;
       schedule[k].ID_Asset:= ID_Asset;
       schedule[k].LastCheck:= Now;
       schedule[k].Timeout:= 0;
      end;

     if not schedule[k].Active then
      begin
       schedule[k].LastCheck:= ScheduleDate;
       schedule[k].Timeout:= Timeout;
       schedule[k].DueDate:= DueDate;
       schedule[k].Active:= true;
      end
     else
      begin
       if DueDate < schedule[k].DueDate then
        begin
         schedule[k].Timeout:= Timeout;
         schedule[k].DueDate:= DueDate;
        end;
      end;

     if s = '' then s:= IntToStr(ID_Schedule) else s += ', ' + IntToStr(ID_Schedule);

     ds.Next;
    end;

   if s <> '' then ds_brm['brm'].Execute(Format('UPDATE "ProcessorSchedule" SET "Checked" = 1 WHERE "ID" IN (%s)', [s]));
  finally
   FreeAndNil(ds);
  end;

  //  call for matching orders
  for i:= 0 to schedule_count - 1 do
   begin
    if not schedule[i].Active then continue;

    if schedule[i].DueDate <= Now then
     begin
      ds:= nil;
      try
       ds_brm['brm'].DataConnector.BeginTransaction;
       try
        ds:= ds_brm['brm'].Query(Format('EXEC "MatchOrdersDISPONIBIL" %d', [schedule[i].ID_Asset]));
        ds_brm['brm'].DataConnector.CommitTransaction;
       except
        ds_brm['brm'].DataConnector.RollbackTransaction;
       end;
       if ds = nil then continue;
       if ds.IsEmpty then continue;

       schedule[i].Active:= ds.FieldByName('Active').AsBoolean;
       schedule[i].LastCheck:= ds.FieldByName('LastCheck').AsDateTime;
       schedule[i].Timeout:= ds.FieldByName('Timeout').AsInteger;
       schedule[i].DueDate:= ds.FieldByName('DueDate').AsDateTime;
      finally
       FreeAndNil(ds);
      end;
     end;
   end;
 finally
  inTimer:= false;
 end;
end;

initialization
 inTimer:= false;
end.

