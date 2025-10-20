# 🛰️ IoT Sensor Event Dashboard

센서 데이터를 **Kafka → SQL Server → WinUI 대시보드**로 실시간 수집·저장·조회할 수 있는 IoT 모니터링 시스템입니다.  
실시간 이벤트 스트림 처리, 오류 검출, 로그 시각화를 주요 목표로 개발되었습니다.

---

## 📁 프로젝트 개요

- **프로젝트명:** IoT Sensor Event Dashboard  
- **개발 기간:** 2025.09 ~ 2025.10  
- **개발 목적:**  
  실시간 IoT 센서 데이터를 스트림으로 수집하고,  
  정상 이벤트와 오류 이벤트를 구분하여 데이터베이스에 저장 및 대시보드로 시각화

---

## 🧩 기술 스택

### 🖥️ Client (Desktop)
- **Framework:** WinUI 3 (.NET 8)
- **Windows App SDK:** 1.8.251003001  
- **Target:** `net8.0-windows10.0.19041.0`  
- **UI 구조:** `NavigationView` + `Frame` + `Page`  
- **주요 구성:** 실시간 이벤트 페이지, 데이터 조회 페이지, 환경설정 페이지

### ⚙️ Service / Storage
- **메시지 브로커:** Apache Kafka  
- **Consumer 라이브러리:** Confluent.Kafka (v2.4.0)  
- **데이터베이스:** Microsoft SQL Server  
- **ORM / DB 드라이버:** Microsoft.Data.SqlClient (v5.2.0)  
- **설정 관리:** Microsoft.Extensions.Configuration.Json (v8.0.0)

### 📦 주요 NuGet 패키지
- Microsoft.WindowsAppSDK `1.8.251003001`
- Microsoft.Windows.SDK.BuildTools `10.0.26100.6584`
- Confluent.Kafka `2.4.0`
- Microsoft.Data.SqlClient `5.2.0`
- Newtonsoft.Json `13.0.3`
- CommunityToolkit.WinUI.UI.Controls.DataGrid `7.1.2`

---

## 📊 주요 기능

### 🔹 실시간 데이터 수집
- Kafka Consumer를 통해 JSON 센서 데이터 실시간 수신  
- 유효성 검증 후 SensorEvent 테이블에 저장  
- 파싱 실패 시 IngestError 테이블에 별도 저장  

### 🔹 실시간 로그 모니터링
- Consumer 상태(Partition, Offset, MPS 등) 실시간 갱신  
- 최대 5000줄까지 자동 관리되는 로그창  
- 로그 초기화 버튼 제공  

### 🔹 이벤트 조회 / 검색
- 기간, 상태(OK / ERROR / ALL), 키워드 기반 검색  
- SensorEvent + IngestError 통합 조회  
- JSON에서 `deviceId` 자동 추출 (`JSON_VALUE`)  
- 비정상 JSON도 안전 처리 (`ISJSON` 검증 적용)  
- 페이지네이션 지원 (First / Prev / Next / Last)

### 🔹 자동 날짜 갱신
- 시작일: 고정 `2025-10-01`  
- 종료일: 매일 자정 자동 갱신 (`DispatcherTimer` 기반)


