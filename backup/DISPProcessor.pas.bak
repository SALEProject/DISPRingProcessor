unit DISPProcessor;

{$mode objfpc}{$H+}

interface

uses
 Classes, SysUtils, bitVariants, bitDataSource, DateUtils, DOM, XMLRead, XMLWrite,
 db, math;

const cns_int_ID_Bursary: integer = 4;

type TOrderDirection = (odir_Buy, odir_Sell);
     TOrderMatch = record
                    ID_OrderMatch: integer;
                    MatchDate: TDateTime;
                    Orders: array of integer;
                    NodeData: pointer;//TTreeNode;

                    marked: boolean;
                   end;

     TOrderRec = record
                  ID_Order: integer;
                  ID_Agency: integer;
                  ID_Broker: integer;
                  ID_Client: integer;
                  OrderDate: TDateTime;
                  Direction: TOrderDirection;
                  Quantity: double;
                  Price: double;
                  NullPrice: boolean;
                  PartialFlag: boolean;
                  ExpirationDate: TDateTime;

                  matches: array of TOrderMatch;

                  NodeData: pointer;//TTreeNode;
                  row: integer;
                 end;
     TOrderRecArray = array of TOrderRec;

     TTradeParameters = record
                         PartialFlagChangeAllowed: boolean;
                         InitialPriceMandatory: boolean;
                         InitialPriceMaintenance: boolean;
                         DiminishedQuantityAllowed: boolean;
                         DiminishedPriceAllowed: boolean;
                         DeltaT: integer;
                         DeltaT1: integer;
                        end;

type TDISPProcessor = class
                      private
                       FStatusFileName: shortstring;
                       FDataSource: TbitDataSource;

                       FBursaryCode: string;
                       FID_Ring: integer;
                       FID_Asset: integer;
                       FID_RingSession: integer;
                       FID_AssetSession: integer;
                       FID_InitialOrder: integer;


                       function GetOrdersCount: integer;

                      public
                       function FindOrder(ID_Order: integer): integer;
                       function FindOrderbyNodeData(NodeData: pointer): integer;

                      public
                       Orders: array of TOrderRec;
                       TradeParameters: TTradeParameters;
                       DBNow: TDateTime;

                       constructor Create(StatusFileName: shortstring);
                       destructor Destroy; override;

                       procedure ReadStatus;
                       procedure WriteStatus;

                       function AddOrder(ID_Order: integer; ID_Agency: integer; ID_Broker: integer; ID_Client: integer; OrderDate: TDateTime;
                                         Direction: TOrderDirection; Quantity: double; Price: double; NullPrice: boolean;
                                         PartialFlag: boolean; ExpirationDate: TDateTime): integer;
                       function RemoveOrder(ID_Order: integer): integer;

                       function FindMatch(ID_Order: integer; const match: array of integer): integer; overload;
                       function FindMatch(ID_Order: integer; ID_OrderMatch: integer): integer; overload;
                       function AddMatch(ID_Order: integer; const match: array of integer): integer;
                       procedure RemoveMatch(ID_Order: integer; ID_OrderMatch: integer);
                       function UnmarkMatch(ID_Order: integer; index: integer): boolean;

                       procedure Process;

                       property StatusFileName: shortstring read FStatusFileName write FStatusFileName;
                       property DataSource: TbitDataSource read FDataSource write FDataSource;
                       property BursaryCode: string read FBursaryCode write FBursaryCode;
                       property ID_Ring: integer read FID_Ring write FID_Ring;
                       property ID_Asset: integer read FID_Asset write FID_Asset;
                       property ID_RingSession: integer read FID_RingSession write FID_RingSession;
                       property ID_AssetSession: integer read FID_AssetSession write FID_AssetSession;
                       property ID_InitialOrder: integer read FID_InitialOrder write FID_InitialOrder;
                       property OrdersCount: integer read GetOrdersCount;

                      public //  db section
                       procedure Refresh_Orders;
                       function GetRingSession(ID: integer): integer;
                       function GetAssetSession(ID: integer): integer;
                       function GetAssetSessionStatus(ID_Session: integer): shortstring;
                       function GetTradeParameters(ID_Session: integer): TTradeParameters;
                       function isAgreed(ID_AST: integer; ID_Client: integer; ID_AgreedClient: integer): boolean;
                       procedure InsertMatches;
                       procedure DeleteMarkedMatches;
                       function GetTransactionOnOrder(ID_Order: integer): integer;
                       function ExecuteTransaction(ID_RSession, ID_ASession, ID_BuyOrder, ID_SellOrder: integer;
                                                 Quantity: double; Price: double): integer;
                       procedure SetEvent(Resource: string; EventType: string; ID_Resource: integer; ID_LinkedResource: integer);
                       procedure SetNotification(ID_Broker, ID_Client, ID_Transation: integer; Quantity, Price: double);
                       procedure AddToJournal(Operation: string; ID_Broker: integer; ID_Agency: integer; ID_Client: integer;
                                              ID_Order: integer; Quantity: double; Price: double);
                       procedure CloseSession;
                      end;

implementation

 function TDISPProcessor.GetOrdersCount: integer;
  begin
   result:= Length(Orders);
  end;

 function TDISPProcessor.FindOrder(ID_Order: integer): integer;
  var b: boolean;
      i: integer;
  begin
   result:= -1;
   b:= false;
   i:= -1;
   while not b and (i < Length(Orders) - 1) do
    begin
     i += 1;
     if Orders[i].ID_Order = ID_Order then b:= true;
    end;

   if b then result:= i;
  end;

 function TDISPProcessor.FindOrderbyNodeData(NodeData: pointer): integer;
  var b: boolean;
      i: integer;
  begin
   result:= -1;
   b:= false;
   i:= -1;
   while not b and (i < Length(Orders) - 1) do
    begin
     i += 1;
     if Orders[i].NodeData = NodeData then b:= true;
    end;

   if b then result:= i;
   end;

 constructor TDISPProcessor.Create(StatusFileName: shortstring);
  begin
   FStatusFileName:= StatusFileName;
  end;

 destructor TDISPProcessor.Destroy;
  begin
   inherited;
  end;

 function GetXMLExpression(Node: TDomNode): string;
  begin
   result:= '';

   if node.ChildNodes.Count = 1 then
    if node.ChildNodes.Item[0].NodeName = '#text' then
     result:= node.ChildNodes.Item[0].NodeValue;
  end;

 procedure TDISPProcessor.ReadStatus;

  function Parse_Direction(s: string): TOrderDirection;
   begin
    result:= odir_Buy;
    if Trim(UpperCase(s)) = 'SELL' then result:= odir_Sell;
   end;

  function Parser_Order(node: TDOMNode): TOrderRec;
   var i: integer;
       xml_node: TDOMNode;
       node_name: shortstring;
   begin
    FillChar(result, sizeof(result), 0);

    for i:= 0 to node.ChildNodes.Count - 1 do
     begin
      xml_node:= node.ChildNodes.Item[i];
      node_name:= Trim(UpperCase(xml_node.NodeName));

      if node_name = 'ID_ORDER'             then result.ID_Order:= StrToInt(GetXMLExpression(xml_node));
      if node_name = 'ID_BROKER'            then result.ID_Broker:= StrToInt(GetXMLExpression(xml_node));
      if node_name = 'ID_CLIENT'            then result.ID_Client:= StrToInt(GetXMLExpression(xml_node));
      if node_name = 'ORDERDATE'            then result.OrderDate:= StrToDateTime(GetXMLExpression(xml_node));
      if node_name = 'DIRECTION'            then result.Direction:= Parse_Direction(GetXMLExpression(xml_node));
      if node_name = 'QUANTITY'             then result.Quantity:= StrToFloat(GetXMLExpression(xml_node));
      if node_name = 'PRICE'                then result.Price:= StrToFloat(GetXMLExpression(xml_node));

      xml_node:= nil;
     end;

   end;

  var filename: string;
      FS: TFileStream;
      doc: TXMLDocument;
      xml_node: TDOMNode;
      node_name: shortstring;
      i, k: integer;
  begin
   SetLength(Orders, 0);

   filename:= ParamStr(0);
   filename:= IncludeTrailingPathDelimiter(ExtractFilePath(filename)) + 'GNRingProcessor_status.xml';

   doc:= TXMLDocument.Create;
   try
    FS:= TFileStream.Create(filename, fmOpenRead);

    try
     ReadXMLFile(doc, FS);
    except
    end;
   finally
    FreeAndNil(FS);
   end;

   for i:= 0 to doc.DocumentElement.ChildNodes.Count - 1 do
    begin
     xml_node:= doc.DocumentElement.ChildNodes.Item[i];
     node_name:= Trim(UpperCase(xml_node.NodeName));

     if node_name = 'ORDER' then
      begin
       k:= Length(Orders);
       SetLength(Orders, k + 1);
       Orders[k]:= Parser_Order(xml_node);
      end;

     xml_node:= nil;
    end;

   FreeAndNil(doc);
  end;

 procedure TDISPProcessor.WriteStatus;
  var filename: shortstring;
      ms: TMemoryStream;
      FS: TFileStream;
      doc: TXMLDocument;
      doc_node, node_order, node_matches, node_match, node: TDOMElement;
      node_matchorders, node_matchorder: TDOMElement;
      i, j, k: integer;
  begin
   try
    doc:= TXMLDocument.Create;

    doc_node:= doc.CreateElement('GNRingProcessor');
    doc.AppendChild(doc_node);

    for i:= 0 to Length(Orders) - 1 do
     begin
      node_order:= doc.CreateElement('Order');
      doc_node.AppendChild(node_order);

      node:= doc.CreateElement('ID_Order');
      node.AppendChild(doc.CreateTextNode(IntToStr(Orders[i].ID_Order)));
      node_order.AppendChild(node);

      node:= doc.CreateElement('ID_Broker');
      node.AppendChild(doc.CreateTextNode(IntToStr(Orders[i].ID_Broker)));
      node_order.AppendChild(node);

      node:= doc.CreateElement('ID_Client');
      node.AppendChild(doc.CreateTextNode(IntToStr(Orders[i].ID_Client)));
      node_order.AppendChild(node);

      node:= doc.CreateElement('OrderDate');
      node.AppendChild(doc.CreateTextNode(DateTimeToStr(Orders[i].OrderDate)));
      node_order.AppendChild(node);

      node:= doc.CreateElement('Direction');
      case Orders[i].Direction of
       odir_Buy : node.AppendChild(doc.CreateTextNode('BUY'));
       odir_Sell: node.AppendChild(doc.CreateTextNode('SELL'));
      end;
      node_order.AppendChild(node);

      node:= doc.CreateElement('Quantity');
      node.AppendChild(doc.CreateTextNode(FloatToStr(Orders[i].Quantity)));
      node_order.AppendChild(node);

      node:= doc.CreateElement('Price');
      node.AppendChild(doc.CreateTextNode(FloatToStr(Orders[i].Price)));
      node_order.AppendChild(node);

      node_matches:= doc.CreateElement('OrderMatches');
      node_order.AppendChild(node_matches);
      for j:= 0 to Length(Orders[i].matches) - 1 do
       begin
        node_match:= doc.CreateElement('OrderMatch');
        node_matches.AppendChild(node_match);

        node:= doc.CreateElement('ID_OrderMatch');
        node.AppendChild(doc.CreateTextNode(IntToStr(Orders[i].matches[j].ID_OrderMatch)));
        node_match.AppendChild(node);

        node_matchorders:= doc.CreateElement('Orders');
        node_match.AppendChild(node_matchorders);

        for k:= 0 to Length(Orders[i].matches[j].Orders) - 1 do
         begin
          node_matchorder:= doc.CreateElement('Order');
          node_matchorder.AppendChild(doc.CreateTextNode(IntToStr(Orders[i].matches[j].Orders[k])));
          node_matchorders.AppendChild(node_matchorder);
         end;
       end;
     end;

    ms:= TMemoryStream.Create;
    WriteXMLFile(doc, ms);
   finally
    doc.Free;
   end;

   if ms = nil then Exit;
   if ms.Size = 0 then Exit;

   try
    FS:= nil;
    try
     FS:= TFileStream.Create(FStatusFileName, fmCreate or fmShareDenyNone);

     ms.Seek(0, soFromBeginning);
     FS.CopyFrom(ms, ms.Size);
    except
    end;
   finally
    FreeAndNil(FS);
    FreeAndNil(ms);
   end;
  end;

 function TDISPProcessor.AddOrder(ID_Order: integer; ID_Agency: integer; ID_Broker: integer; ID_Client: integer; OrderDate: TDateTime;
                                 Direction: TOrderDirection; Quantity: double; Price: double; NullPrice: boolean;
                                 PartialFlag: boolean; ExpirationDate: TDateTime): integer;
  var k: integer;

  begin
   k:= Length(Orders);
   SetLength(Orders, k + 1);
   Orders[k].ID_Order:= ID_Order;
   Orders[k].ID_Agency:= ID_Agency;
   Orders[k].ID_Broker:= ID_Broker;
   Orders[k].ID_Client:= ID_Client;
   Orders[k].OrderDate:= OrderDate;
   Orders[k].Direction:= Direction;
   Orders[k].Quantity:= Quantity;
   Orders[k].Price:= Price;
   Orders[k].NullPrice:= NullPrice;
   Orders[k].PartialFlag:= PartialFlag;
   Orders[k].ExpirationDate:= ExpirationDate;

   result:= k;
  end;

 function TDISPProcessor.RemoveOrder(ID_Order: integer): integer;
  var i: integer;
  begin
   result:= FindOrder(ID_Order);
   if result = -1 then Exit;

   for i:= result to Length(Orders) - 2 do Orders[i]:= Orders[i + 1];
   SetLength(Orders, Length(Orders) - 1);
  end;

 function TDISPProcessor.FindMatch(ID_Order: integer; const match: array of integer): integer;
  var b, bb: boolean;
      idx, i, j, k: integer;
  begin
   result:= -1;
   idx:= FindOrder(ID_Order);
   if idx = -1 then Exit;

   b:= false;
   i:= -1;
   while not b and (i < Length(Orders[idx].matches) - 1) do
    begin
     i += 1;

     if Length(Orders[idx].matches[i].Orders) = Length(match) then
      begin
       b:= true;
       for j:= 0 to Length(match) - 1 do
        begin
         bb:= false;
         k:= -1;
         while not bb and (k < Length(Orders[idx].matches[i].Orders) - 1) do
          begin
           k += 1;
           if Orders[idx].matches[i].Orders[k] = match[j] then bb:= true;
          end;

         if not bb then b:= false;
        end;
      end;
    end;

   if b then result:= i;
  end;

 function TDISPProcessor.FindMatch(ID_Order: integer; ID_OrderMatch: integer): integer;
  var idx, i: integer;
      b: boolean;
  begin
   result:= -1;
   idx:= FindOrder(ID_Order);
   if idx = -1 then Exit;

   b:= false;
   i:= -1;
   while not b and (i < Length(Orders[idx].matches) - 1) do
    begin
     i += 1;

     if Orders[idx].matches[i].ID_OrderMatch = ID_OrderMatch then b:= true;
    end;

   if b then result:= i;
  end;

 function TDISPProcessor.AddMatch(ID_Order: integer; const match: array of integer): integer;
  var idx, i, k: integer;
  begin
   result:= -1;
   idx:= FindOrder(ID_Order);
   if idx = -1 then Exit;

   k:= Length(Orders[idx].matches);
   SetLength(Orders[idx].matches, k + 1);

   FillChar(Orders[idx].matches[k], sizeof(TOrderMatch), 0);
   Orders[idx].matches[k].MatchDate:= Now;
   SetLength(Orders[idx].matches[k].Orders, Length(match));
   for i:= 0 to Length(match) - 1 do Orders[idx].matches[k].Orders[i]:= match[i];
   result:= k;

   SetEvent('DeltaT1', 'reset', ID_Asset, ID_Order);
  end;

 procedure TDISPProcessor.RemoveMatch(ID_Order: integer; ID_OrderMatch: integer);
  var idx, k, i: integer;
  begin
   idx:= FindOrder(ID_Order);
   if idx = -1 then Exit;

   k:= FindMatch(ID_Order, ID_OrderMatch);
   if k = -1 then Exit;

   for i:= k to Length(Orders[idx].matches) - 2 do Orders[idx].matches[i]:= Orders[idx].matches[i + 1];
   SetLength(Orders[idx].matches, Length(Orders[idx].matches) - 1);
  end;

 function TDISPProcessor.UnmarkMatch(ID_Order: integer; index: integer): boolean;
  var idx: integer;
  begin
   result:= false;
   idx:= FindOrder(ID_Order);
   if idx = -1 then Exit;

   if (index < 0) or (index >= Length(Orders[idx].matches)) then Exit;
   Orders[idx].matches[index].marked:= false;
   result:= true;
  end;

 procedure TDISPProcessor.Process;

  function FindPerfectMatch(order: TOrderRec; shortlist: array of TOrderRec): integer;
   var b: boolean;
       i: integer;
   begin
    result:= -1;

    b:= false;
    i:= -1;
    while not b and (i < Length(shortlist) - 1) do
     begin
      i += 1;

      if (order.Quantity = shortlist[i].Quantity) and (order.Price = shortlist[i].Price) then b:= true;
     end;

    if b then result:= i;
   end;

  function FindIntersection(order: TOrderRec; shortlist: array of TOrderRec): integer;
   var b: boolean;
       i, idx_bestmatch: integer;
   begin
    idx_bestmatch:= -1;
    result:= -1;

    i:= -1;
    while (i < Length(shortlist) - 1) do
     begin
      i += 1;
      b:= false;

      if (order.Quantity = shortlist[i].Quantity) then b:= true;

      if b then
       case order.Direction of
        odir_Buy : if shortlist[i].Price > order.Price then b:= false;
        odir_Sell: if shortlist[i].Price < order.Price then b:= false;
       end;

      if b then
       begin
        if idx_bestmatch = -1 then idx_bestmatch:= i
        else if shortlist[idx_bestmatch].OrderDate < shortlist[i].OrderDate then idx_bestmatch:= i;
       end;
     end;

    result:= idx_bestmatch;
   end;

  function FindBestPrice(order: TOrderRec; shortlist: array of TOrderRec): integer;
   var b: boolean;
       i: integer;
       best_price: double;
       best_choice: integer;
   begin
    result:= -1;
    if Length(shortlist) = 0 then Exit;

    b:= false;
    best_choice:= -1;
    case order.Direction of
     odir_Buy : best_price:= order.Price * 1.2;
     odir_Sell: best_price:= order.Price * 0.8;
    end;

    for i:= 0 to Length(shortlist) - 1 do
     case order.Direction of
      odir_Buy : if (order.Quantity = shortlist[i].Quantity)
                 and (shortlist[i].Price < best_price) then
                  begin
                   b:= true;
                   best_price:= shortlist[i].Price;
                   best_choice:= i;
                  end;
      odir_Sell: if (order.Quantity = shortlist[i].Quantity)
                 and (shortlist[i].Price > best_price) then
                  begin
                   b:= true;
                   best_price:= shortlist[i].Price;
                   best_choice:= i;
                  end;
     end;

    if b then result:= best_choice;
   end;

  function FindBestQuantity(order: TOrderRec; shortlist: array of TOrderRec): integer;
   var b: boolean;
       i: integer;
       best_price, best_quantity: double;
       best_choice: integer;
   begin
    result:= -1;
    if Length(shortlist) = 0 then Exit;

    b:= false;
    best_choice:= -1;
    best_quantity:= order.Quantity * 0.8;
    case order.Direction of
     odir_Buy : best_price:= order.Price * 1.2;
     odir_Sell: best_price:= order.Price * 0.8;
    end;

    for i:= 0 to Length(shortlist) - 1 do
     case order.Direction of
      odir_Buy : if (abs(shortlist[i].Quantity - order.Quantity) < abs(order.Quantity - best_quantity))
                 and (shortlist[i].Price < best_price) then
                  begin
                   b:= true;
                   best_price:= shortlist[i].Price;
                   best_quantity:= shortlist[i].Quantity;
                   best_choice:= i;
                  end;
      odir_Sell: if (abs(shortlist[i].Quantity - order.Quantity) < abs(order.Quantity - best_quantity))
                 and (shortlist[i].Price > best_price) then
                  begin
                   b:= true;
                   best_price:= shortlist[i].Price;
                   best_quantity:= shortlist[i].Quantity;
                   best_choice:= i;
                  end;
     end;

    if b then result:= best_choice;
   end;


 var i, j, k: integer;
     order, o: TOrderRec;
     shortlist, match: array of TOrderRec;
     short_bid, short_ask: array of TOrderRec;
     matchids: array of integer;
     b, bb: boolean;
     idx_perfect, idx_intersection, idx_bestprice, idx_bestquantity: integer;
     PriceSettlement: double;
     ID_Transaction: integer;
     Status: shortstring;
     filled_qty, remaining_qty: double;
     tr_qty, tr_price, set_price: double;
     set_NullPrice: boolean;
 begin
  if (ID_RingSession <= 0) or (ID_AssetSession <= 0) then Exit;
  //if (ID_InitialOrder <> 1321) then Exit;

  //  check current status of the trading session
  //  if still not closed do not search for matches
  Status:= GetAssetSessionStatus(FID_AssetSession);
  if (Status <> 'Opened') and (Status <> 'PreClosed') then Exit;

  //  check if we already have a transaction on initial order
  {ID_Transaction:= GetTransactionOnOrder(FID_InitialOrder);
  if ID_Transaction <> 0 then Exit;}

  //  mark all previous matches for deletion
  for i:= 0 to Length(Orders) - 1 do
   for j:= 0 to Length(Orders[i].matches) - 1 do
    Orders[i].matches[j].marked:= true;

  //  create queues for bid and aks
  SetLength(short_bid, 0);
  SetLength(short_ask, 0);
  set_Price:= 0;
  set_NullPrice:= false;
  for i:= 0 to Length(Orders) - 1 do
   begin
    case Orders[i].Direction of
     odir_Buy : begin
                 k:= Length(short_bid);
                 SetLength(short_bid, k + 1);
                 short_bid[k]:= Orders[i];
                end;
     odir_Sell: begin;
                 k:= Length(short_ask);
                 SetLength(short_ask, k + 1);
                 short_ask[k]:= Orders[i];
                end
    end;

    if Orders[i].ID_Order = FID_InitialOrder then
     begin
      set_Price:= Orders[i].Price;
      set_NullPrice:= Orders[i].NullPrice;
     end;
   end;

  try
   if (Length(short_bid) = 0) or (Length(short_ask) = 0) then Exit;

   //  Sort each list accordingly
   repeat
    b:= true;

    for i:= 0 to Length(short_bid) - 2 do
     if (short_bid[i].Price < short_bid[i + 1].Price) or
        ((short_bid[i].Price = short_bid[i + 1].Price) and (short_bid[i].OrderDate > short_bid[i + 1].OrderDate)) then
      begin
       o:= short_bid[i];
       short_bid[i]:= short_bid[i + 1];
       short_bid[i + 1]:= o;
       b:= false;
      end;
   until b;

   repeat
    b:= true;

    for j:= 0 to Length(short_ask) - 2 do
     if (short_ask[j].Price > short_ask[j + 1].Price) or
        ((short_ask[j].Price = short_ask[j + 1].Price) and (short_ask[j].OrderDate > short_ask[j + 1].OrderDate)) then
      begin
       o:= short_ask[j];
       short_ask[j]:= short_ask[j + 1];
       short_ask[j + 1]:= o;
       b:= false;
      end;
   until b;

   //  take each order from the first short list and try to match it to one from the
   //  other list until something is found
   tr_qty:= 0;
   tr_price:= 0;

   b:= false;
   i:= -1;
   while not b and (((FID_InitialOrder > 0) and (not set_NullPrice){(set_Price > 0)}) or (FID_InitialOrder = 0))
               and (i < Length(short_bid) - 1) do
    begin
     i += 1;

     //  check each order from the opposite short list
     j:= -1;
     while not b and (j < Length(short_ask) - 1) do
      begin
       j += 1;

       order:= short_bid[i];
       o:= short_ask[j];

       if order.Price < o.Price then Continue;
       if order.ID_Client = o.ID_Client then Continue;


       if (order.Quantity = o.Quantity) or
          (o.PartialFlag and (order.Quantity < o.Quantity)) then
        begin
         b:= true;
         tr_qty:= order.Quantity;
         tr_price:= o.Price;
        end
       else if order.PartialFlag and (order.Quantity > o.Quantity) then
        begin
         b:= true;
         tr_qty:= o.Quantity;
         tr_price:= o.Price;
        end;

       if b and (o.ID_Order = FID_InitialOrder) then tr_price:= order.Price;
      end;
    end;

   //  if a match is found then do it and exit afterwards
   if b then
    begin
     k:= FindMatch(order.ID_Order, [o.ID_Order]);
     if k = -1 then k:= AddMatch(order.ID_Order, [o.ID_Order])
     else UnmarkMatch(order.ID_Order, k);

     //  try to execute transaction
     i:= FindOrder(order.ID_Order);
     if (DBNow - Orders[i].matches[k].MatchDate) * 24 * 60 >= TradeParameters.DeltaT1 then
      begin
       case order.Direction of
        odir_Buy : ID_Transaction:= ExecuteTransaction(ID_RingSession, ID_AssetSession, order.ID_Order, o.ID_Order, tr_qty, tr_price);
        odir_Sell: ID_Transaction:= ExecuteTransaction(ID_RingSession, ID_AssetSession, o.ID_Order, order.ID_Order, tr_qty, tr_price);
       end;
       //UnmarkMatch(order.ID_Order, k);
       Orders[i].matches[k].marked:= true;

       SetNotification(order.ID_Broker, order.ID_Client, ID_Transaction, tr_qty, tr_price);
       SetNotification(o.ID_Broker, o.ID_Client, ID_Transaction, tr_qty, tr_price);
       AddToJournal('transaction', order.ID_Broker, order.ID_Agency, order.ID_Client, order.ID_Order, tr_qty, tr_price);
      end;
    end;

  finally
   DeleteMarkedMatches;
   InsertMatches;
  end;

  //  nothing after this matter
  Exit;


  //  search initial order in list
(*  i:= -1;
  b:= false;
  while not b and (i < Length(Orders) - 1) do
   begin
    i += 1;
    if Orders[i].ID_Order = FID_InitialOrder then b:= true;
   end;

  if i = -1 then Exit; //  not found, exiting
  order:= Orders[i];
  SetLength(shortlist, 0);

  //---------------------------------------------------------------------------
  //  filter the other orders and generate a subset of possibilities
  for j:= 0 to Length(Orders) - 1 do
   if i <> j then
    begin
     o:= Orders[j];

     if order.Direction = o.Direction then Continue; //  cannot match orders of the same direction
     if order.ID_Client = o.ID_Client then Continue; //  orders cannot come from the same client

     //  eliminate orders that do not fit in price range
     if order.Price <> 0 then
      case order.Direction of
       odir_Buy : if o.Price > order.Price then Continue;
       odir_SELL: if o.Price < order.Price then Continue;
      end;

     //  check if order o is from an accepted partner
     if isAgreed(FID_Asset, order.ID_Client, o.ID_Client) then
      begin
       //  add to shortlist
       k:= Length(shortlist);
       SetLength(shortlist, k + 1);
       shortlist[k]:= o;
      end;
    end;

  try
   if Length(shortlist) = 0 then Exit; //  no variants were found

   //----------------------------------------------------------------------------
   //  sort the short list by price and date
   repeat
    b:= true;
    for j:= 0 to Length(shortlist) - 2 do
     begin
      bb:= false;
      case order.Direction of
       odir_Buy : if (shortlist[j].Price > shortlist[j + 1].Price) or
                 ((shortlist[j].Price = shortlist[j + 1].Price) and (shortlist[j].OrderDate > shortlist[j + 1].OrderDate)) then bb:= true;
       odir_Sell: if (shortlist[j].Price < shortlist[j + 1].Price) or
                 ((shortlist[j].Price = shortlist[j + 1].Price) and (shortlist[j].OrderDate > shortlist[j + 1].OrderDate)) then bb:= true;
      end;

      if bb then
       begin
        o:= shortlist[j];
        shortlist[j]:= shortlist[j + 1];
        shortlist[j + 1]:= o;
        b:= false;
       end;
     end;
   until b;

   filled_qty:= 0;
   remaining_qty:= order.Quantity;
   for j:= 0 to Length(shortlist) - 1 do
    begin
     b:= false;
     tr_qty:= 0;
     tr_price:= 0;

     if (shortlist[j].Quantity = remaining_qty) or
        (shortlist[j].PartialFlag and (shortlist[j].Quantity > remaining_qty)) then
      begin
       b:= true;
       tr_qty:= remaining_qty;
       tr_price:= shortlist[j].Price;
      end
     else if order.PartialFlag and (shortlist[j].Quantity < remaining_qty) then
      begin
       b:= true;
       tr_qty:= shortlist[j].Quantity;
       tr_price:= shortlist[j].Price;
      end;

     //  a possible match has been found
     if b and (order.Price <> 0) then
      begin
       filled_qty += tr_qty;
       remaining_qty -= tr_qty;

       k:= FindMatch(order.ID_Order, [shortlist[idx_perfect].ID_Order]);
       if k = -1 then k:= AddMatch(order.ID_Order, [shortlist[idx_perfect].ID_Order])
       else UnmarkMatch(order.ID_Order, k);

       {k:= FindMatch(shortlist[idx_perfect].ID_Order, [order.ID_Order]);
       if k = -1 then AddMatch(shortlist[idx_perfect].ID_Order, [order.ID_Order])
       else UnmarkMatch(shortlist[idx_perfect].ID_Order, k);}

       //  try to execute transaction
       if (DBNow - Orders[i].matches[k].MatchDate) * 24 * 60 >= TradeParameters.DeltaT1 then
        begin
         case order.Direction of
          odir_Buy : ID_Transaction:= ExecuteTransaction(ID_RingSession, ID_AssetSession, order.ID_Order, shortlist[idx_perfect].ID_Order, tr_qty, tr_price);
          odir_Sell: ID_Transaction:= ExecuteTransaction(ID_RingSession, ID_AssetSession, shortlist[idx_perfect].ID_Order, order.ID_Order, tr_qty, tr_price);
         end;
         SetNotification(order.ID_Broker, order.ID_Client, ID_Transaction, tr_qty, tr_price);
         SetNotification(shortlist[idx_perfect].ID_Broker, shortlist[idx_perfect].ID_Client, ID_Transaction, tr_qty, tr_price);

         //  remove orders from lists
         RemoveOrder(order.ID_Order); i -= 1;
         if RemoveOrder(shortlist[idx_perfect].ID_Order) <= i then i -= 1;

         CloseSession;
        end;
      end;
    end;     *)


   //---------------------------------------------------------------------------
   //  search for a perfect match in order of dates
   (*idx_perfect:= FindPerfectMatch(order, shortlist);
   idx_intersection:= FindIntersection(order, shortlist);

   if idx_perfect > -1 then
    begin
     //  search and add/update match in the matches list
     k:= FindMatch(order.ID_Order, [shortlist[idx_perfect].ID_Order]);
     if k = -1 then k:= AddMatch(order.ID_Order, [shortlist[idx_perfect].ID_Order])
     else UnmarkMatch(order.ID_Order, k);

     {k:= FindMatch(shortlist[idx_perfect].ID_Order, [order.ID_Order]);
     if k = -1 then AddMatch(shortlist[idx_perfect].ID_Order, [order.ID_Order])
     else UnmarkMatch(shortlist[idx_perfect].ID_Order, k);}

     //  try to execute transaction
     if (DBNow - Orders[i].matches[k].MatchDate) * 24 * 60 >= TradeParameters.DeltaT1 then
      begin
       case order.Direction of
        odir_Buy : ID_Transaction:= ExecuteTransaction(ID_RingSession, ID_AssetSession, order.ID_Order, shortlist[idx_perfect].ID_Order, order.Quantity, order.Price);
        odir_Sell: ID_Transaction:= ExecuteTransaction(ID_RingSession, ID_AssetSession, shortlist[idx_perfect].ID_Order, order.ID_Order, order.Quantity, order.Price);
       end;
       SetNotification(order.ID_Broker, order.ID_Client, ID_Transaction, order.Quantity, order.Price);
       SetNotification(shortlist[idx_perfect].ID_Broker, shortlist[idx_perfect].ID_Client, ID_Transaction, order.Quantity, order.Price);

       //  remove orders from lists
       RemoveOrder(order.ID_Order); i -= 1;
       if RemoveOrder(shortlist[idx_perfect].ID_Order) <= i then i -= 1;

       CloseSession;
      end;
    end
   else if idx_intersection > -1 then
    begin
     //  search and add/update match in the matches list
     k:= FindMatch(order.ID_Order, [shortlist[idx_intersection].ID_Order]);
     if k = -1 then k:= AddMatch(order.ID_Order, [shortlist[idx_intersection].ID_Order])
     else UnmarkMatch(order.ID_Order, k);

     {k:= FindMatch(shortlist[idx_intersection].ID_Order, [order.ID_Order]);
     if k = -1 then AddMatch(shortlist[idx_intersection].ID_Order, [order.ID_Order])
     else UnmarkMatch(shortlist[idx_intersection].ID_Order, k);}

     PriceSettlement:= order.Price;
     if shortlist[idx_intersection].OrderDate > order.OrderDate then PriceSettlement:= shortlist[idx_intersection].Price;

     //  try to execute transaction
     //  try to execute transaction
     if (DBNow - Orders[i].matches[k].MatchDate) * 24 * 60 >= TradeParameters.DeltaT1 then
      begin
       case order.Direction of
        odir_Buy : ExecuteTransaction(ID_RingSession, ID_AssetSession, order.ID_Order, shortlist[idx_intersection].ID_Order, order.Quantity, PriceSettlement);
        odir_Sell: ExecuteTransaction(ID_RingSession, ID_AssetSession, shortlist[idx_intersection].ID_Order, order.ID_Order, order.Quantity, PriceSettlement);
       end;
       SetNotification(order.ID_Broker, order.ID_Client, ID_Transaction, order.Quantity, PriceSettlement);
       SetNotification(shortlist[idx_perfect].ID_Broker, shortlist[idx_perfect].ID_Client, ID_Transaction, order.Quantity, PriceSettlement);

       //  remove orders from lists
       RemoveOrder(order.ID_Order); i -= 1;
       if RemoveOrder(shortlist[idx_intersection].ID_Order) <= i then i -= 1;

       CloseSession;
      end;
    end;
  finally
   DeleteMarkedMatches;
   InsertMatches;
  end;*)

 end;




 //-----------------------------------------------------------------------------
 //  DB Section
 //-----------------------------------------------------------------------------

 procedure TDISPProcessor.Refresh_Orders;
  var ds, ds_OrderMatches, ds_OrderMatchDetails: TDataSet;
      ID_Order, ID_Agency, ID_Broker, ID_Client, ID_OrderMatch, ID_OrderMatchDetail: integer;
      i, j, k: integer;
      OrderDate: TDateTime;
      Direction: TOrderDirection;
      Quantity, Price: double;
      PartialFlag: boolean;
      ExpirationDate: TDateTime;
      dt_start, dt_end: TDateTime;
      CombinationsAccepted: boolean;
      ID_GNType: integer;
      ds_now: TDataSet;
      v: Variant;
      //node, node_match, node_matchdet: TTreeNode;
  begin
   if FDataSource = nil then Exit;

   SetLength(Orders, 0);

   ds:= nil;
   try
    ds:= FDataSource['brm'].Query(Format('SELECT O.*, IsNUll((SELECT SUM(T."Quantity") FROM "Transactions" T WHERE (T."ID_BuyOrder" = O."ID") OR (T."ID_SellOrder" = O."ID")), 0) AS "TransactedQuantity" ' +
                                         'FROM "Orders" O ' +
                                         'WHERE (O."isActive" = 1) AND (O."ID_RingSession" = %d) AND (O."ID_AssetSession" = %d)' +
                                         'ORDER BY O."Date"', [FID_RingSession, FID_AssetSession]));
    if ds = nil then Exit;

    ds.First;
    while not ds.Eof do
     begin
      ID_Order:= ds.FieldByName('ID').AsInteger;
      ID_Agency:= ds.FieldByName('ID_Agency').AsInteger;
      ID_Broker:= ds.FieldByName('ID_Broker').AsInteger;
      ID_Client:= ds.FieldByName('ID_Client').AsInteger;
      OrderDate:= ds.FieldByName('Date').AsDateTime;

      case ds.FieldByName('Direction').AsBoolean of
       false: Direction:= odir_Buy;
       true : Direction:= odir_Sell;
      end;

      Quantity:= ds.FieldByName('Quantity').AsFloat - ds.FieldByName('TransactedQuantity').AsFloat;
      Price:= ds.FieldByName('Price').AsFloat;
      v:= ds.FieldByName('Price').Value;
      PartialFlag:= ds.FieldByName('PartialFlag').AsBoolean;
      ExpirationDate:= ds.FieldByName('ExpirationDate').AsDateTime;

      i:= AddOrder(ID_Order, ID_Agency, ID_Broker, ID_Client, OrderDate, Direction, Quantity, Price, (v = Null), PartialFlag, ExpirationDate);

      ds_OrderMatches:= FDataSource['brm'].Query('SELECT * FROM "OrderMatches" WHERE "ID_Order" = ' + IntToStr(ID_Order));
      if ds_OrderMatches = nil then continue;

      ds_OrderMatches.First;
      while not ds_OrderMatches.Eof do
       begin
        ID_OrderMatch:= ds_OrderMatches.FieldByName('ID').AsInteger;

        //node_match:= tv_Orders.Items.AddChild(node, 'ID_OrderMatch: ' + IntToStr(ID_OrderMatch));

        j:= Length(Orders[i].matches);
        SetLength(Orders[i].matches, j + 1);
        Orders[i].matches[j].ID_OrderMatch:= ID_OrderMatch;
        Orders[i].matches[j].MatchDate:= ds_OrderMatches.FieldByName('Date').AsDateTime;
        //Orders[i].matches[j].node:= node_match;

        ds_OrderMatchDetails:= FDataSource['brm'].Query('SELECT * FROM "OrderMatchDetails" WHERE "ID_OrderMatch" = ' + IntToStr(ID_OrderMatch));
        if ds_OrderMatchDetails = nil then continue;

        ds_OrderMatchDetails.First;
        while not ds_OrderMatchDetails.Eof do
         begin
          ID_OrderMatchDetail:= ds_OrderMatchDetails.FieldByName('ID').AsInteger;

          //node_matchdet:= tv_Orders.Items.AddChild(node_match, 'ID_Order: ' + IntToStr(ds_OrderMatchDetails.FieldByName('ID_Order').AsInteger));

          k:= Length(Orders[i].matches[j].Orders);
          SetLength(Orders[i].matches[j].Orders, k + 1);
          Orders[i].matches[j].Orders[k]:= ds_OrderMatchDetails.FieldByName('ID_Order').AsInteger;

          ds_OrderMatchDetails.Next;
         end;

        ds_OrderMatches.Next;
       end;

      ds.Next;
     end;

   finally
    FreeAndNil(ds);
   end;

   TradeParameters:= GetTradeParameters(ID_AssetSession);
   //if not ds.IsEmpty then ID_Selected:= ds.FieldByName('ID').AsInteger;

   //  get current timestamp from database
   ds_now:= nil;
   ds_now:= FDataSource['brm'].Query('SELECT GetDate() AS "Now"');
   if ds_now <> nil then
    begin
     if not ds_now.IsEmpty then DBNow:= ds_now.FieldByName('Now').AsDateTime;

     FreeAndNil(ds_now);
    end;
  end;

 function TDISPProcessor.GetRingSession(ID: integer): integer;
  var ds_session: TDataSet;
  begin
   result:= 0;
   if FDataSource = nil then Exit;

   ds_session:= nil;
   try
    ds_session:= FDataSource['brm'].Query(Format('SELECT TOP 1 * FROM "RingSessions" WHERE "ID_Ring" = %d ORDER BY "Date" DESC', [ID]));
    if ds_session = nil then Exit;
    if ds_session.IsEmpty then Exit;

    if int(ds_session.FieldByName('Date').AsDateTime) <> int(Now) then Exit;
    if (ds_session.FieldByName('Status').AsString <> 'Opened') then Exit;

    result:= ds_session.FieldByName('ID').AsInteger;
   finally
    FreeAndNil(ds_session);
   end;
  end;

 function TDISPProcessor.GetAssetSession(ID: integer): integer;
  var ds_session: TDataSet;
  begin
   result:= 0;
   if FDataSource = nil then Exit;

   ds_session:= nil;
   try
    ds_session:= FDataSource['brm'].Query(Format('SELECT TOP 1 * FROM "AssetSessions" WHERE "ID_Asset" = %d ORDER BY "Date" DESC', [ID]));
    if ds_session = nil then Exit;
    if ds_session.IsEmpty then Exit;

    if int(ds_session.FieldByName('Date').AsDateTime) <> int(Now) then Exit;
    if (ds_session.FieldByName('Status').AsString <> 'Opened') then Exit;

    result:= ds_session.FieldByName('ID').AsInteger;
   finally
    FreeAndNil(ds_session);
   end;
  end;

 function TDISPProcessor.GetAssetSessionStatus(ID_Session: integer): shortstring;
  var ds: TDataset;
      str_sql: string;
  begin
   result:= '';
   ds:= nil;

   try
    str_sql:= 'SELECT * FROM "AssetSessions" WHERE "ID" = ' + IntToStr(ID_AssetSession);
    ds:= FDataSource['brm'].Query(str_sql);
    if ds = nil then Exit;
    if ds.IsEmpty then Exit;

    result:= ds.FieldByName('Status').AsString;
   finally
    FreeAndNil(ds);
   end;
  end;

 function TDISPProcessor.GetTradeParameters(ID_Session: integer): TTradeParameters;
  var str_sql: string;
      ds: TDataset;
  begin
   FillChar(result, sizeof(result), 0);

   ds:= nil;

   try
    str_sql:= 'SELECT ' +
              'CONVERT(BIT, IsNull(TPA."PartialFlagChangeAllowed", 0)) AS "PartialFlagChangeAllowed", ' +
              'CONVERT(BIT, IsNull(TPA."InitialPriceMandatory", 0)) AS "InitialPriceMandatory", ' +
              'CONVERT(BIT, IsNull(TPA."InitialPriceMaintenance", 0)) AS "InitialPriceMaintenance", ' +
              'CONVERT(BIT, IsNull(TPA."DiminishedQuantityAllowed", 0)) AS "DiminishedQuantityAllowed", ' +
              'CONVERT(BIT, IsNull(TPA."DiminishedPriceAllowed", 0)) AS "DiminishedPriceAllowed", ' +
              'CONVERT(INT, IsNull(TPS."DeltaT", 0)) AS "DeltaT", ' +
              'CONVERT(INT, IsNull(TPS."DeltaT1", 0)) AS "DeltaT1" ' +
              'FROM "AssetSessions" S ' +
              'LEFT JOIN "Assets" A ON (S."ID_Asset" = A."ID") LEFT JOIN "TradeParameters" TPA ON (A."ID" = TPA."ID_Asset") ' +
              'LEFT JOIN "AssetTypes" AT ON (A."ID_AssetType" = AT."ID") LEFT JOIN "TradeParameters" TPT ON (AT."ID" = TPT."ID_AssetType") ' +
              'LEFT JOIN "Orders" O ON (A."ID_InitialOrder" = O."ID") LEFT JOIN "TradeParameters" TPO ON (O."ID" = TPO."ID_Order") ' +
              'LEFT JOIN "AssetSchedules" SS ON (S."ID_AssetSchedule" = SS."ID") LEFT JOIN "TradeParameters" TPS ON (SS."ID" = TPS."ID_AssetSchedule") ' +
              'WHERE S."ID" = %d';
    ds:= FDataSource['brm'].Query(Format(str_sql, [ID_AssetSession]));
    if ds = nil then Exit;
    if ds.IsEmpty then Exit;

    result.PartialFlagChangeAllowed:= ds.FieldByName('PartialFlagChangeAllowed').AsBoolean;
    result.InitialPriceMandatory:= ds.FieldByName('InitialPriceMandatory').AsBoolean;
    result.InitialPriceMaintenance:= ds.FieldByName('InitialPriceMaintenance').AsBoolean;
    result.DiminishedQuantityAllowed:= ds.FieldByName('DiminishedQuantityAllowed').AsBoolean;
    result.DiminishedPriceAllowed:= ds.FieldByName('DiminishedPriceAllowed').AsBoolean;
    result.DeltaT:= ds.FieldByName('DeltaT').AsInteger;
    result.DeltaT1:= ds.FieldByName('DeltaT1').AsInteger;
   finally
    FreeAndNil(ds);
   end;
  end;

 function TDISPProcessor.isAgreed(ID_AST: integer; ID_Client: integer; ID_AgreedClient: integer): boolean;
  var ds: TDataSet;
  begin
   result:= true;

   ds:= nil;
   try
    ds:= FDataSource['brm'].Query(Format('SELECT * FROM "AgreedClients" WHERE ("ID_Asset" = %d) AND ("ID_Client" = %d) AND ("ID_AgreedClient" = %d)', [ID_AST, ID_Client, ID_AgreedClient]));
    if ds = nil then Exit;
    if ds.IsEmpty then result:= true
    else
     begin
      result:= ds.FieldByName('isAgreed').AsBoolean;
      if not result then if not ds.FieldByName('isApproved').AsBoolean then result:= true;
     end;
   finally
    FreeAndNil(ds);
   end;

   if result then
    begin
     ds:= nil;
     try
      ds:= FDataSource['brm'].Query(Format('SELECT * FROM "AgreedClients" WHERE ("ID_Asset" = %d) AND ("ID_Client" = %d) AND ("ID_AgreedClient" = %d)', [ID_AST, ID_AgreedClient, ID_Client]));
      if ds = nil then Exit;
      if ds.IsEmpty then result:= true
      else
       begin
        result:= ds.FieldByName('isAgreed').AsBoolean;
        if not result then if not ds.FieldByName('isApproved').AsBoolean then result:= true;
       end;
     finally
      FreeAndNil(ds);
     end;
    end;
  end;

 procedure TDISPProcessor.InsertMatches;
  var i, j, k: integer;
      ds_identity: TDataSet;
  begin
   if FDataSource = nil then Exit;

   for i:= 0 to Length(Orders) - 1 do
    begin
     for j:= 0 to Length(Orders[i].matches) - 1 do
      if (Orders[i].matches[j].ID_OrderMatch = 0) and not Orders[i].matches[j].marked then
       begin
        FDataSource['brm'].Execute(Format('INSERT INTO "OrderMatches" ("ID_Order") VALUES (%d)', [Orders[i].ID_Order]));

        ds_identity:= nil;
        try
         ds_identity:= FDataSource['brm'].Query('SELECT SCOPE_IDENTITY() AS "Identity"');
         Orders[i].matches[j].ID_OrderMatch:= ds_identity.FieldByName('Identity').AsInteger;
        finally
         FreeAndNil(ds_identity);
        end;

        for k:= 0 to Length(Orders[i].matches[j].Orders) - 1 do
         FDataSource['brm'].Execute(Format('INSERT INTO "OrderMatchDetails" ("ID_OrderMatch", "ID_Order") VALUES (%d, %d)', [Orders[i].matches[j].ID_OrderMatch, Orders[i].matches[j].Orders[k]]));
       end;
    end;
  end;

 procedure TDISPProcessor.DeleteMarkedMatches;
  var i, j: integer;
  begin
   if FDataSource = nil then Exit;

   for i:= 0 to Length(Orders) - 1 do
    begin
     j:= 0;
     while j < Length(Orders[i].matches) do
      begin
       if Orders[i].matches[j].marked then
        begin
         FDataSource['brm'].Execute(Format('DELETE FROM "OrderMatchDetails" WHERE "ID_OrderMatch" = %d', [Orders[i].matches[j].ID_OrderMatch]));
         FDataSource['brm'].Execute(Format('DELETE FROM "OrderMatches" WHERE "ID" = %d', [Orders[i].matches[j].ID_OrderMatch]));
         RemoveMatch(Orders[i].ID_Order, Orders[i].matches[j].ID_OrderMatch);
         j -= 1;
        end;

       j += 1;
      end;
    end;
  end;

 function TDISPProcessor.GetTransactionOnOrder(ID_Order: integer): integer;
  var str_sql: string;
      ds: TDataset;
  begin
   result:= -1;

   ds:= nil;
   try
    str_sql:= 'SELECT * FROM "Transactions" WHERE ("ID_BuyOrder" = %d) OR ("ID_SellOrder" = %d)';
    ds:= FDataSource['brm'].Query(Format(str_sql, [ID_Order, ID_Order]));

    if ds = nil then Exit;
    if ds.IsEmpty then begin result:= 0; Exit; end;
    result:= ds.FieldByName('ID').AsInteger;
   finally
    FreeAndNil(ds);
   end;
  end;

 function TDISPProcessor.ExecuteTransaction(ID_RSession, ID_ASession, ID_BuyOrder, ID_SellOrder: integer;
                                            Quantity: double; Price: double): integer;
  var vl_params: TbitVariantList;
      idx_buy, idx_sell: integer;
      ID_Transaction: integer;
      ds_identity: TDataSet;
  begin
   if FDataSource = nil then Exit;

   idx_buy:= FindOrder(ID_BuyOrder);
   idx_sell:= FindOrder(ID_SellOrder);
   if (idx_buy = -1) or (idx_sell = -1) then Exit;

   vl_params:= TbitVariantList.Create;
   vl_params.Add('@prm_ID_RingSession').AsInteger:= ID_RingSession;
   vl_params.Add('@prm_ID_AssetSession').AsInteger:= ID_AssetSession;
   vl_params.Add('@prm_ID_Ring').AsInteger:= FID_Ring;
   vl_params.Add('@prm_ID_Asset').AsInteger:= FID_Asset;
   vl_params.Add('@prm_ID_BuyOrder').AsInteger:= ID_BuyOrder;
   vl_params.Add('@prm_ID_SellOrder').AsInteger:= ID_SellOrder;
   vl_params.Add('@prm_Quantity').AsDouble:= Quantity;
   vl_params.Add('@prm_Price').AsDouble:= Price;

   FDataSource['brm'].Execute('INSERT INTO "Transactions" ' +
                              '("ID_Ring", "ID_RingSession", "ID_Asset", "ID_AssetSession", "ID_BuyOrder", "ID_SellOrder", "Quantity", "Price") ' +
                              'VALUES ' +
                              '(@prm_ID_Ring, @prm_ID_RingSession, @prm_ID_Asset, @prm_ID_AssetSession, @prm_ID_BuyOrder, @prm_ID_SellOrder, @prm_Quantity, @prm_Price)', vl_params);

   ds_identity:= nil;
   try
    ds_identity:= FDataSource['brm'].Query('SELECT SCOPE_IDENTITY() AS "Identity"');
    ID_Transaction:= ds_identity.FieldByName('Identity').AsInteger;
   finally
    FreeAndNil(ds_identity);
   end;

   //  mark orders as transacted
   if Orders[idx_buy].Quantity - Quantity = 0 then
    FDataSource['brm'].Execute(Format('UPDATE "Orders" SET "isTransacted" = 1, "isActive" = 0 WHERE "ID" = %d', [ID_BuyOrder]))
   else
    FDataSource['brm'].Execute(Format('UPDATE "Orders" SET "isTransacted" = 0, "isActive" = 1 WHERE "ID" = %d', [ID_BuyOrder]));

   if Orders[idx_sell].Quantity - Quantity = 0 then
    FDataSource['brm'].Execute(Format('UPDATE "Orders" SET "isTransacted" = 1, "isActive" = 0 WHERE "ID" = %d', [ID_SellOrder]))
   else
    FDataSource['brm'].Execute(Format('UPDATE "Orders" SET "isTransacted" = 0, "isActive" = 1 WHERE "ID" = %d', [ID_SellOrder]));

   FreeAndNil(vl_params);

   result:= ID_Transaction;
  end;

 procedure TDISPProcessor.SetEvent(Resource: string; EventType: string; ID_Resource: integer; ID_LinkedResource: integer);
  var str_sql: string;
  begin
   str_sql:= 'INSERT INTO "Events" ("Resource", "EventType", "ID_Resource", "ID_LinkedResource") ' +
             'VALUES (''%s'', ''%s'', %d, %d)';
   FDataSource['brm'].Execute(Format(str_sql, [Resource, EventType, ID_Resource, ID_LinkedResource]));
  end;

 procedure TDISPProcessor.SetNotification(ID_Broker, ID_Client, ID_Transation: integer; Quantity, Price: double);
  var ds: TDataSet;
      ID_RecipientAgency, ID_RecipientUser: integer;
      Subject, Body: string;
  begin
   ds:= FDataSource['brm'].Query(Format('SELECT A.* FROM "Brokers" B ' +
                                        'LEFT JOIN "Agencies" A ON (B."ID_Agency" = A."ID") ' +
                                        'LEFT JOIN "Clients" C ON (A."ID" = C."ID_Agency") ' +
                                        'WHERE (B."ID" = %d) AND (C."ID" = %d)', [ID_Broker, ID_Client]));
   if ds = nil then Exit;
   if ds.IsEmpty then Exit;
   ID_RecipientAgency:= ds.FieldByName('ID').AsInteger;
   FreeAndNil(ds);

   ds:= FDataSource['brm'].Query(Format('SELECT U.* FROM "Brokers" B ' +
                                        'LEFT JOIN "Users" U ON (B."ID_User" = U."ID") ' +
                                        'WHERE B."ID_Agency" = %d', [ID_RecipientAgency]));
   if ds = nil then Exit;
   if ds.IsEmpty then Exit;
   ds.First;
   while not ds.EOF do
    begin
     Subject:= Format('Tranzactionare %fx%f', [Quantity, Price]);
     Body:= Subject;
     ID_RecipientUser:= ds.FieldByName('ID').AsInteger;
     FDataSource['brm'].Execute(Format('INSERT INTO "Notifications" ' +
                                       '("ID_Bursary", "ID_SenderUser", "ID_SenderAgency", "ID_RecipientUser", "ID_RecipientAgency", "Subject", "Body", "BodyHTML") ' +
                                       'VALUES ' +
                                       '(%d, %d, %d, %d, %d, ''%s'', ''%s'', ''%s'')',
                                       [2, 0, 0, ID_RecipientUser, ID_RecipientAgency, Subject, Body, Body]));

     ds.Next;
    end;
  end;

 procedure TDISPProcessor.AddToJournal(Operation: string; ID_Broker: integer; ID_Agency: integer; ID_Client: integer;
                                       ID_Order: integer; Quantity: double; Price: double);
  var str_sql: string;
  begin
   str_sql:= 'INSERT INTO "Journal" ("Date", "Operation", "ID_User", "ID_Broker", "ID_Agency", "ID_Client", ' +
             '"ID_Bursary", "ID_Ring", "ID_Asset", "ID_Order", "Quantity", "Price") VALUES ' +
             '(GetDate(), ''%s'', 0, %d, %d, %d, %d, %d, %d, %d, %f, %f)';
   str_sql:= Format(str_sql, [Operation, ID_Broker, ID_Agency, ID_Client, cns_int_ID_Bursary, ID_Ring, ID_Asset, ID_Order, Quantity, Price ]);

   FDataSource['brm'].Execute(str_sql);
  end;

 procedure TDISPProcessor.CloseSession;
  var i: integer;
      str_sql: string;
  begin
   for i:= 0 to Length(Orders) - 1 do
    begin
     str_sql:= 'UPDATE "Orders" SET "isActive" = 0 WHERE "ID" = %d';
     FDataSource['brm'].Execute(Format(str_sql, [Orders[i].ID_Order]));
    end;

   str_sql:= 'UPDATE "AssetSessions" SET "Status" = ''Closed'' WHERE "ID" = %d';
   FDataSource['brm'].Execute(Format(str_sql, [FID_AssetSession]));

   str_sql:= 'UPDATE "Assets" SET "Code" = "Code" WHERE "ID" = %d';
   FDataSource['brm'].Execute(Format(str_sql, [FID_Asset]));
  end;

end.

